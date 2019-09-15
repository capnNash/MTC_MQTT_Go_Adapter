package main

//>----< Import necessary libraries >-----<
import (
	"encoding/json"
	"flag"
	cb "github.com/clearblade/Go-SDK"
	"encoding/base64"
	//cb "Go-SDK"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	//"reflect"
	"fmt"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/clearblade/paho.mqtt.golang"
	//"github.com/hashicorp/logutils"
)

//>-----< varibles for connecting to the platform >-----<
var (
	platformURL         string //Defaults to http://localhost:9000
	messagingURL        string //Defaults to localhost:1883
	sysKey              string
	sysSec              string
	deviceName          string //Defaults to mqttBridgeAdapter
	activeKey           string
	logLevel            string //Defaults to info
	adapterConfigCollID string
	config              adapterConfig
	iotRightClient            *cb.DeviceClient
	cbSentMessages      SentMessages
)

const (
	qos = 0 // qos to use for all sub/pubs
)

//>-----< Adapter configuration from the platform >-----<
type adapterConfig struct {
	BrokerConfig mqttBroker `json:"adapter_settings"`
	TopicRoot    string     `json:"topic_root"`
}


//>------< Mqtt broker for the Lora Network >------<
type mqttBroker struct {
	MessagingURL string   `json:"messagingURL"`
	Username     string   `json:"username"`
	Password     string   `json:"password"`
	Topics       []string `json:"topics"`
	Client       mqtt.Client
}

type SentKey struct {
	Topic, Message string
}

type SentMessages struct {
	Mutex    *sync.Mutex
	Messages map[SentKey]int
}

//>---< Init functon >----<
func init() {
	flag.StringVar(&sysKey, "systemKey", "", "system key (required)")
	flag.StringVar(&sysSec, "systemSecret", "", "system secret (required)")
	flag.StringVar(&deviceName, "deviceName", "mqttBridgeAdapter", "name of device (optional)")
	flag.StringVar(&activeKey, "password", "", "password (or active key) for device authentication (required)")
	flag.StringVar(&platformURL, "platformURL", "http://localhost:9000", "platform url (optional)")
	flag.StringVar(&messagingURL, "messagingURL", "localhost:1883", "messaging URL (optional)")
	flag.StringVar(&logLevel, "logLevel", "info", "The level of logging to use. Available levels are 'debug, 'info', 'warn', 'error', 'fatal' (optional)")
	flag.StringVar(&adapterConfigCollID, "adapterConfigCollectionID", "", "The ID of the data collection used to house adapter configuration (required)")
}

func usage() {
	log.Printf("Usage: mqttBridgeAdapter [options]\n\n")
	flag.PrintDefaults()
}

//>-----< Validate flags >-----<
func validateFlags() {
	flag.Parse()
	if sysKey == "" || sysSec == "" || activeKey == "" || adapterConfigCollID == "" {
		log.Println("ERROR - Missing required flags")
		flag.Usage()
		os.Exit(1)
	}
}

//>------< Function starts here >-------<
func main() {
	log.Println("Starting mqttBridgeAdapter...")
	flag.Usage = usage
	validateFlags()
	rand.Seed(time.Now().UnixNano())

	logfile, err := os.OpenFile("/var/log/mqttBridgeAdapter", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err.Error())
	}

	defer logfile.Close()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// filter := &logutils.LevelFilter{
	// 	Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"},
	// 	MinLevel: logutils.LogLevel(strings.ToUpper(logLevel)),
	// 	Writer:   logfile,
	// }
	//log.SetOutput(filter)
	iotRightClient = initIoTRightClient()
	for config.BrokerConfig.Client, err = initOtherMQTT(); err != nil; {
		log.Println("[ERROR] Failed to initialize other MQTT client, trying again in 1 minute")
		time.Sleep(time.Duration(time.Minute * 1))
		config.BrokerConfig.Client, err = initOtherMQTT()
	}

	//create map that stores sent messages, need this because we have no control of topic structure on other MQTT broker,
	// so we can't break messages out into incoming/outgoing topics like the IoTRight side does
	cbSentMessages = SentMessages{
		Mutex:    &sync.Mutex{},
		Messages: make(map[SentKey]int),
	}

	//on iotright we subscribe to all outgoing topics prefaced with topic root
	log.Println("[INFO] Subscribing to outgoing iotright topic")
	var cbSubChannel <-chan *mqttTypes.Publish
	for cbSubChannel, err = iotRightClient.Subscribe(config.TopicRoot+"/outgoing/#", qos); err != nil; {

	}
	go cbMessageListener(cbSubChannel)

	//on other mqtt we subscribe to the provided topics, or all topics if nothing is provided
	if len(config.BrokerConfig.Topics) == 0 {
		log.Println("[INFO] No topics provided, subscribing to all topics for other MQTT broker")
		config.BrokerConfig.Client.Subscribe("#", qos, otherMessageHandler)
	} else {
		log.Printf("[INFO] Subscribing to remote topics: %+v\n", config.BrokerConfig.Topics)
		for _, element := range config.BrokerConfig.Topics {
			config.BrokerConfig.Client.Subscribe(element, qos, otherMessageHandler)
		}
	}

	for {
		log.Println("[INFO] Listening for messages..")
		time.Sleep(time.Duration(time.Second * 60))
	}
}

func cbMessageListener(onPubChannel <-chan *mqttTypes.Publish) {
	for {
		select {
		case message, ok := <-onPubChannel:
			if ok {
				// message published to cb broker
				if len(message.Topic.Split) >= 3 {
					log.Printf("[DEBUG] cbMessageListener - message received topic: %s message: %s\n", message.Topic.Whole, string(message.Payload))
					topicToUse := strings.Join(message.Topic.Split[2:], "/")
					cbSentMessages.Mutex.Lock()
					cbSentMessages.Messages[SentKey{topicToUse, string(message.Payload)}]++
					cbSentMessages.Mutex.Unlock()
					config.BrokerConfig.Client.Publish(topicToUse, qos, false, message.Payload)
				} else {
					log.Printf("[DEBUG] cbMessageListener - Unexpected topic for message from IoTRight Broker: %s\n", message.Topic.Whole)
				}
			}
		}
	}
}


//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<
//>==============================================< Parser for Netvox Sensors >==============================================<
//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>=======================================< Default structs (Incoming and Outgoing) >===============================<
type msgPayload struct {
	// Rssi int
	Data string
}


//>==================================================================================================<

//>====================================< UTILITY FUNCTIONS >======================================<

//>-----------< Convert Dec to Hex >---------<
func dec2hex(dec int) string {
  color := dec
	return fmt.Sprintf("%02x", color)
}

//>-----------< Convert Hex to Dec >-----------<
func hex2dec(hex string) int64 {
	dec, _ := strconv.ParseInt("0x"+hex, 0, 16)
	return dec
}

//>-----------< Convert two byte data in decimal >------------<
func getTwoByteDecimal(byte1 uint8,byte2 uint8) float64{
	Hexdec := hex2dec(""+dec2hex(int(byte1))+""+dec2hex(int(byte2)))
	return float64(Hexdec)
}

//>============================================< UTILITY END >=======================================================<




//>===============================================< Ellenex Differential Pressure PDT2-L Sensor Parser >=================================================<
//>-------------< PDT2-L Sensor struct >-------------<
type pdt2l_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Pressure string `json:"pressure"`
		Temperature string `json:"temperature"`
	} `json:"data"`
}

//>--------------< pdt2l sensor function >---------------<
func pdt2l(sensorData []uint8) string {
	var pdt2l pdt2l_json
	pdt2l.Volt = fmt.Sprintf("%.2f", (float64(sensorData[7]) / 10.0))
	pdt2l.State = "-1"
	pdt2l.Signal = "-1"
	pdt2l.Type = "Differential_Pressure_Sensor"
	pdt2l.Sensorid = "pdt2l"
	//FIXME:add condition here for report type. future work
	pdt2l.Data.Pressure = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[3], sensorData[4]))
	pdt2l.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[5], sensorData[6])/10)
	pdt2l.Datetime = time.Now().String()
	data, err := json.Marshal(pdt2l)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< PDT2L End >==================================================================<

//>========================================< Identify Sensor >===================================<

func parseSensorData(sensorData []uint8) string{
	log.Println("Testing the ellenex parser")
	log.Println(sensorData)
	sensorType := dec2hex(int(getTwoByteDecimal(sensorData[0], sensorData[1])))
	var x string
	switch sensorType{
	case "312" :
		log.Println("PDT2L")
		x = pdt2l(sensorData)
	default:
		log.Println("default")
	}
	return x
}

//>============================================XXXXXXXXXXXX=============================================<



//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<
//>=============================================< End Parse Ellenex Sensor >=====================================================<
//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<





//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<
//>=============================================< Multitech Message handler from Lora Devices >=================================<
//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

func otherMessageHandler(client mqtt.Client, msg mqtt.Message) {
	cbSentMessages.Mutex.Lock()
	n := cbSentMessages.Messages[SentKey{msg.Topic(), string(msg.Payload())}]
	if n == 1 {
		delete(cbSentMessages.Messages, SentKey{msg.Topic(), string(msg.Payload())})
		cbSentMessages.Mutex.Unlock()
		log.Println("[DEBUG] otherMessageHandler - ignoring message because it came from IoTRight")
		return
	} else if n > 1 {
		cbSentMessages.Messages[SentKey{msg.Topic(), string(msg.Payload())}]--
		cbSentMessages.Mutex.Unlock()
		log.Println("[DEBUG] otherMessageHandler - ignoring message because it came from IoTRight")
		return
	}
	cbSentMessages.Mutex.Unlock()
	//log.Printf("[DEBUG] otherMessageHandler - message received topic: %s message: %s\n", msg.Topic(), string(msg.Payload()))
	topicToUse := config.TopicRoot + "/incoming/" + msg.Topic()
	message := string(msg.Payload())



	bytes := []byte(message)
	var outputMessage msgPayload
	json.Unmarshal(bytes, &outputMessage)
	tempData := outputMessage.Data
	log.Println("String data")
	log.Println(tempData)
	sensorData, err := base64.StdEncoding.DecodeString(tempData)
	if err != nil {  
		log.Println("Falied to Perfrom URL Encoding", sensorData)  
		return  
	}
	//decoded message here [1 19 .....]
	//Recognize the sensor
	log.Println("Sensor Data ",parseSensorData(sensorData))
	payload := []byte(parseSensorData(sensorData))

	if err := iotRightClient.Publish(topicToUse, payload, qos); err != nil {
		log.Printf("[ERROR] otherMessageHandler - failed to forward message to IoTRight: %s\n", err.Error())
	}
}


//>----------------< Initialize and authenticate with IoTRight platform >----------------< 
func initIoTRightClient() *cb.DeviceClient {
	client := cb.NewDeviceClientWithAddrs(platformURL, messagingURL, sysKey, sysSec, deviceName, activeKey)
	log.Println("[INFO] initIoTRightClient - Authenticating with IotRight")
	for err := client.Authenticate(); err != nil; {
		log.Printf("[ERROR] initIoTRightClient - Error authenticating IotRight: %s\n", err.Error())
		log.Println("[ERROR] initIoTRightClient - Will retry in 1 minute...")
		time.Sleep(time.Duration(time.Minute * 1))
		err = client.Authenticate()
	}

	log.Println("[INFO] initIoTRightClient - Fetching adapter config")
	setAdapterConfig(client)

	log.Println("[INFO] initIoTRightClient - Initializing IoTRight MQTT")
	callbacks := cb.Callbacks{OnConnectCallback: onCBConnect, OnConnectionLostCallback: onCBDisconnect}
	if err := client.InitializeMQTTWithCallback(deviceName+"-"+strconv.Itoa(randomInt(0, 10000)), "", 30, nil, nil, &callbacks); err != nil {
		log.Fatalf("[FATAL] initIoTRightClient - Unable to initialize MQTT connection with IoTRight: %s", err.Error())
	}

	return client
}


//>-----------< Initialize Multitech's mqtt broker >-----------<
func initOtherMQTT() (mqtt.Client, error) {
	log.Println("[INFO] initOtherMQTT - Initializing Other MQTT")

	opts := mqtt.NewClientOptions()

	opts.AddBroker(config.BrokerConfig.MessagingURL)

	if config.BrokerConfig.Username != "" {
		opts.SetUsername(config.BrokerConfig.Username)
	}

	if config.BrokerConfig.Password != "" {
		opts.SetPassword(config.BrokerConfig.Password)
	}

	opts.SetClientID(deviceName + "-" + strconv.Itoa(randomInt(0, 10000)))
	opts.SetOnConnectHandler(onOtherConnect)
	opts.SetConnectionLostHandler(onOtherDisconnect)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Printf("[ERROR] initOtherMQTT - Unable to connect to other MQTT Broker: %s", token.Error())
		return nil, token.Error()
	}
	log.Println("[INFO] initOtherMQTT - Other MQTT Connected")
	return client, nil
}

func setAdapterConfig(client cb.Client) {
	log.Println("[INFO] setAdapterConfig - Fetching adapter config")

	query := cb.NewQuery()
	query.EqualTo("adapter_name", deviceName)

	log.Println("[DEBUG] setAdapterConfig - Executing query against table " + adapterConfigCollID)
	results, err := client.GetData(adapterConfigCollID, query)
	if err != nil {
		log.Fatalf("[FATAL] setAdapterConfig - Error fetching adapter config: %s", err.Error())
	}

	data := results["DATA"].([]interface{})

	if len(data) == 0 {
		log.Fatalf("[FATAL] - setAdapterConfig - No configuration found for adapter with name: %s", deviceName)
	}

	config = adapterConfig{TopicRoot: "mqtt-bridge-adapter"}

	configData := data[0].(map[string]interface{})
	log.Printf("[DEBUG] setAdapterConfig - fetched config:\n%+v\n", data)
	if configData["topic_root"] != nil {
		config.TopicRoot = configData["topic_root"].(string)
	}
	if configData["adapter_settings"] == nil {
		log.Fatalln("[FATAL] setAdapterConfig - No adapter settings required, this is required")
	}
	var bC mqttBroker
	if err := json.Unmarshal([]byte(configData["adapter_settings"].(string)), &bC); err != nil {
		log.Fatalf("[FATAL] setAdapterConfig - Failed to parse adapter_settings: %s", err.Error())
	}

	config.BrokerConfig = bC

	if config.BrokerConfig.MessagingURL == "" {
		log.Fatalln("[FATAL] setAdapterConfig - No messaging URL defined for broker config adapter_settings")
	}

	log.Printf("[DEBUG] setAdapterConfig - Using adapter settings:\n%+v\n", config)
}

func onCBConnect(client mqtt.Client) {
	log.Println("[DEBUG] onCBConnect - IoTRight MQTT connected")
}

func onCBDisconnect(client mqtt.Client, err error) {
	log.Printf("[DEBUG] onCBDisonnect - IoTRight MQTT disconnected: %s", err.Error())
}

func onOtherConnect(client mqtt.Client) {
	log.Println("[DEBUG] onOtherConnect - Other MQTT connected")
}

func onOtherDisconnect(client mqtt.Client, err error) {
	log.Printf("[DEBUG] onOtherConnect - Other MQTT disconnected: %s", err.Error())
}

func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

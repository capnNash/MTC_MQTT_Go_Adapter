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


//>===================================< R718AB Temperature and Humidity Sensor Parser >====================================<


//>-------------< R718AB Sensor struct >-------------<
type r718ab_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Temperature string `json:"temperature"`
		Humidity string `json:"humidity"`
		} `json:"data"`
}


//>--------------< R718AB sensor function >---------------<
func r718ab(sensorData []uint8) string{
	var r718ab r718ab_json
	r718ab.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718ab.State = "-1"
	r718ab.Signal = "-1"
	r718ab.Type = "Temperature_Humidity_Sensor"
	r718ab.Sensorid = "R718AB"
	r718ab.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])/100.0)
	r718ab.Data.Humidity = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])/100.0) 
	r718ab.Datetime = time.Now().String()

	data, err := json.Marshal(r718ab)
	if err != nil {
		 log.Println("error")
	}
	 fmt.Println(string(data))
	 return string(data)
}


//>=======================================================< R718AB End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718CT2 Thermocouple Sensor Parser >=================================================<

//>-------------< R718CT2 Sensor struct >-------------<
type r718ct2_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Temperature1 string `json:"temperature1"`
		Temperature2 string `json:"temperature2"`
	} `json:"data"`
}

//>--------------< R718CT2 sensor function >---------------<
func r718ct2(sensorData []uint8) string{
	var r718ct2 r718ct2_json
	r718ct2.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718ct2.State = "-1"
	r718ct2.Signal = "-1"
	r718ct2.Type = "Thermocouple_Sensor"
	r718ct2.Sensorid = "R718CT2"
	r718ct2.Data.Temperature1 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])/10.0) 
	r718ct2.Data.Temperature2 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])/10.0) 
	r718ct2.Datetime = time.Now().String()
	data, err := json.Marshal(r718ct2)
	if err != nil {
		 log.Println("error")
	}
	 fmt.Println(string(data))
	 return string(data)
}

//>=======================================================< R718CT2 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718N3 Wireless 3-Phase Current Sensor Parser >=================================================<
//FIXME: Needs clarification on the current multiplier for the sensor
//>-------------< R718N3 Sensor struct >-------------<
type r718n3_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Current1 string `json:"current1"`
		Current2 string `json:"current2"`
		Current3 string `json:"current3"`
	} `json:"data"`
}

//>--------------< R718N3 sensor function >---------------<
func r718n3(sensorData []uint8) string{
	var r718n3 r718n3_json
	r718n3.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718n3.State = "-1"
	r718n3.Signal = "-1"
	r718n3.Type = "3-Phase_Current_Sensor"
	r718n3.Sensorid = "R718N3"

	r718n3.Data.Current1 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])) 
	r718n3.Data.Current2 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])) 
	r718n3.Data.Current3 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8],sensorData[9])) 
	r718n3.Datetime = time.Now().String()
	data, err := json.Marshal(r718n3)
	if err != nil {
		 log.Println("error")
	}
	 fmt.Println(string(data))
	 return string(data)
}

//>=======================================================< R718N3 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718MA Wireless Asset Sensor Parser >=================================================<

//>-------------< R718MA Sensor struct >-------------<
type r718ma_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Rssi string `json:"rssi"`
		Snr string `json:"snr"`
		Heartinterval string `json:"HeartInterval"`
	}`json:"data"`
} 

//>--------------< R718MA sensor function >---------------<
func r718ma(sensorData []uint8) string{
	var r718ma r718ma_json
	r718ma.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718ma.State = "-1"
	r718ma.Signal = "-1"
	r718ma.Type = "Wireless_Asset_Sensor"
	r718ma.Sensorid = "R718MA"
	r718ma.Data.Rssi = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])) 
	r718ma.Data.Snr = fmt.Sprintf("%.2f", (float64(sensorData[6])))
	r718ma.Data.Heartinterval = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[7],sensorData[8])) 
	r718ma.Datetime = time.Now().String()
	data, err := json.Marshal(r718ma)
	if err != nil {
		 log.Println("error")
	}
	 fmt.Println(string(data))
	 return string(data)
}

//>=======================================================< R718MA End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< RB11E Occupancy/Light/Temperature Sensor Parser >=================================================<

//>-------------< RB11E Sensor struct >-------------<
type rb11e_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Temperature string `json:"temperature"`
		Illuminance string `json:"illuminance"`
		Occupancy string `json:"occupancy"`
	}`json:"data"`
} 

//>--------------< RB11E sensor function >---------------<
func rb11e(sensorData []uint8) string{
	var rb11e rb11e_json
	rb11e.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	rb11e.State = "-1"
	rb11e.Signal = "-1"
	rb11e.Type = "Occupancy_Light_Temperature_Sensor"
	rb11e.Sensorid = "RB11E"
	rb11e.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5]) * 0.01) 
	rb11e.Data.Occupancy = fmt.Sprintf("%.2f", (float64(sensorData[8])))
	rb11e.Data.Illuminance = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])) 
	rb11e.Datetime = time.Now().String()
	data, err := json.Marshal(rb11e)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< RB11E End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< RA0715 CO2/Temperature/Humidity Sensor Parser >=================================================<

//>-------------< RA0715 Sensor struct >-------------<
type ra0715_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Pm1_0 string `json:"PM1.0"`
		Pm2_5 string `json:"PM2.5"`
		Pm10 string `json:"PM10"`
	}`json:"data"`
} 

//>--------------< RA0715 sensor function >---------------<
func ra0715(sensorData []uint8) string{
	var ra0715 ra0715_json
	ra0715.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	ra0715.State = "-1"
	ra0715.Signal = "-1"
	ra0715.Type = "CO2_Temperature_Humidity_Sensor"
	ra0715.Sensorid = "RA0715"
	ra0715.Data.Pm1_0 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])) 
	ra0715.Data.Pm2_5 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])) 
	ra0715.Data.Pm10 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8],sensorData[9])) 
	ra0715.Datetime = time.Now().String()
	data, err := json.Marshal(ra0715)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< RA0715 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< RA0716 Dust/Temperature/Humidity Sensor Parser >=================================================<

//>-------------< RA0716 Sensor struct >-------------<
type ra0716_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
			Temperature string `json:"temperature"`
			Humidity string `json:"humidity"`
			Pm2_5 string `json:"PM2.5"`
		}`json:"data"`
} 

//>--------------< RA0716 sensor function >---------------<
func ra0716(sensorData []uint8) string{
	var ra0716 ra0716_json
	ra0716.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	ra0716.State = "-1"
	ra0716.Signal = "-1"
	ra0716.Type = "Dust_Temperature_Humidity_Sensor"
	ra0716.Sensorid = "RA0716"
	ra0716.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5]) * 0.01) 
	ra0716.Data.Humidity = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7]) * 0.01) 
	ra0716.Data.Pm2_5 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8],sensorData[9])) 
	ra0716.Datetime = time.Now().String()
	data, err := json.Marshal(ra0716)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< RA0716 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718PC RS485 Sensor Parser >=================================================<
//TODO: payload data missing
//>-------------< R718PC Sensor struct >-------------<
type r718pc_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
} 

//>--------------< R718PC sensor function >---------------<
func r718pc(sensorData []uint8) string{
	var r718pc r718pc_json
	//r718pc.Default.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718pc.State = "-1"
	r718pc.Signal = "-1"
	r718pc.Type = "RS485_Sensor"
	r718pc.Sensorid = "R718PC"
	r718pc.Datetime = time.Now().String()
	data, err := json.Marshal(r718pc)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R718PC End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718KA2 Wireless 2 input mA current Meter Interface Sensor Parser >=================================================<

//>-------------< R718KA2 Sensor struct >-------------<
type r718ka2_json struct {
	Datetime string `json:"datatimestamp"`
	State string `json:"state"`
	Type string `json:"type"`
	Signal string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt string `json:"voltage"`
	Data struct{
		Current1 string `json:"current1"`
		Current2 string `json:"current2"`
	}`json:"data"`
} 

//>--------------< R718KA2 sensor function >---------------<
func r718ka2(sensorData []uint8) string{
	var r718ka2 r718ka2_json
	r718ka2.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3])/10.0))
	r718ka2.State = "-1"
	r718ka2.Signal = "-1"
	r718ka2.Type = "Wireless_2_input_mA_current_Meter_Interface_Sensor"
	r718ka2.Sensorid = "R718KA2"
	r718ka2.Datetime = time.Now().String()
	r718ka2.Data.Current1 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4],sensorData[5])) 
	r718ka2.Data.Current2 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6],sensorData[7])) 
	data, err := json.Marshal(r718ka2)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R718KA2 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R711 Temperature and Humidity Sensor Parser >=================================================<

//>-------------< RA0716 Sensor struct >-------------<
type r711_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Temperature string `json:"temperature"`
		Humidity    string `json:"humidity"`
	} `json:"data"`
}

//>--------------< R711 sensor function >---------------<
func r711(sensorData []uint8) string {
	var r711 r711_json
	r711.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	r711.State = "-1"
	r711.Signal = "-1"
	r711.Type = "Temperature_Humidity_Sensor"
	r711.Sensorid = "R718AB"
	r711.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5])/100.0)
	r711.Data.Humidity = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6], sensorData[7])/100.0)
	r711.Datetime = time.Now().String()
	data, err := json.Marshal(r711)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R711 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===================================< R718H2 Wireless 2-Input Pulse Counter Interface Sensor Parser >====================================<

//>-------------< R718H2 Sensor struct >-------------<
type r718h2_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Pulse1 string `json:"pulse1"`
		Pulse2    string `json:"pulse2"`
	} `json:"data"`
}

//>--------------< R718H2 sensor function >---------------<
func r718h2(sensorData []uint8) string {
	var r718h2 r718h2_json
	r718h2.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	r718h2.State = "-1"
	r718h2.Signal = "-1"
	r718h2.Type = "Wireless_2-Input_Pulse_Counter_Interface_Sensor"
	r718h2.Sensorid = "R718H2"
	r718h2.Data.Pulse1 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5]))
	r718h2.Data.Pulse2 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6], sensorData[7]))
	r718h2.Datetime = time.Now().String()

	data, err := json.Marshal(r718h2)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R718H2 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718N17 Wireless Single-Phase Current Sensor Parser >=================================================<

//FIXME: Needs clarification on the current multiplier for the sensor
//>-------------< R718N17 Sensor struct >-------------<
type r718n17_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Current string `json:"current"`
	} `json:"data"`
}

//>--------------< R718N17 sensor function >---------------<
func r718n17(sensorData []uint8) string {
	var r718n17 r718n17_json
	r718n17.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	r718n17.State = "-1"
	r718n17.Signal = "-1"
	r718n17.Type = "3-Phase_Current_Sensor"
	r718n17.Sensorid = "R718N3"
	r718n17.Data.Current = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5]))
	r718n17.Datetime = time.Now().String()
	data, err := json.Marshal(r718n17)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R718N17 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R72616A Indoor Multi-function Environment Sensor Parser >=================================================<

//>-------------< R72616A Sensor struct >-------------<
type r72616a_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Temperature string `json:"temperature"`
		Humidity    string `json:"humidity"`
		Pm2_5       string `json:"PM2.5"`
	} `json:"data"`
}

//>--------------< R72616A sensor function >---------------<
func r72616a(sensorData []uint8) string {
	var r72616a r72616a_json
	r72616a.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	r72616a.State = "-1"
	r72616a.Signal = "-1"
	r72616a.Type = "Indoor_Multi-function_Environment_Sensor"
	r72616a.Sensorid = "R72616A"
	r72616a.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5])*0.01)
	r72616a.Data.Humidity = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6], sensorData[7])*0.01)
	r72616a.Data.Pm2_5 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8], sensorData[9]))
	r72616a.Datetime = time.Now().String()
	data, err := json.Marshal(r72616a)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R72616A End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< RA0701 Indoor Multi-function Environment Sensor Parser >=================================================<
//FIXME: Determine the device type for this sensor
//>-------------< RA0701 Sensor struct >-------------<
type ra0701_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		Temperature string `json:"temperature"`
		Humidity    string `json:"humidity"`
		Pm2_5       string `json:"PM2.5"`
	} `json:"data"`
}

//>--------------< RA0701 sensor function >---------------<
func ra0701(sensorData []uint8) string {
	var ra0701 ra0701_json
	ra0701.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	ra0701.State = "-1"
	ra0701.Signal = "-1"
	ra0701.Type = "Indoor_Multi-function_Environment_Sensor"
	ra0701.Sensorid = "RA0701"
	ra0701.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5])*0.01)
	ra0701.Data.Humidity = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6], sensorData[7])*0.01)
	ra0701.Data.Pm2_5 = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8], sensorData[9]))
	ra0701.Datetime = time.Now().String()
	data, err := json.Marshal(ra0701)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< RA0701 End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< R718E Three-Axis Digital Accelerometer & NTC Thermistor Sensor Parser >=================================================<
//FIXME: Fix this parser for additional report type --> See Documentation
//>-------------< R718E Sensor struct >-------------<
type r718e_json struct {
	Datetime string `json:"datatimestamp"`
	State    string `json:"state"`
	Type     string `json:"type"`
	Signal   string `json:"signal"`
	Sensorid string `json:"sensorid"`
	Volt     string `json:"voltage"`
	Data     struct {
		AccelerationX string `json:"accelerationX"`
		AccelerationY string `json:"accelerationY"`
		AccelerationZ string `json:"accelerationZ"`
	} `json:"data"`
}

//>--------------< R718e sensor function >---------------<
func r718e(sensorData []uint8) string {
	var r718e r718e_json
	r718e.Volt = fmt.Sprintf("%.2f", (float64(sensorData[3]) / 10.0))
	r718e.State = "-1"
	r718e.Signal = "-1"
	r718e.Type = "Three-Axis_Digital_Accelerometer_NTC_Thermistor_Sensor"
	r718e.Sensorid = "R718E"
	//add condition here for report type
	r718e.Data.AccelerationX = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[4], sensorData[5]))
	r718e.Data.AccelerationY = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[6], sensorData[7]))
	r718e.Data.AccelerationZ = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[8], sensorData[9]))
	r718e.Datetime = time.Now().String()
	data, err := json.Marshal(r718e)
	if err != nil {
		log.Println("error")
	}
	fmt.Println(string(data))
	return string(data)
}

//>=======================================================< R718E End >==================================================================<

//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<

//>===============================================< Ellenex Differential Pressure PDT2-L Sensor Parser >=================================================<
//FIXME: Need to test this
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
	//add condition here for report type
	pdt2l.Data.Pressure = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[3], sensorData[4]))
	pdt2l.Data.Temperature = fmt.Sprintf("%.2f", getTwoByteDecimal(sensorData[5], sensorData[6]))
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
	log.Println(sensorData)
	sensorType := dec2hex(int(sensorData[1]))
	var x string
	switch sensorType{
	case "13" :
		log.Println("R718AB")
		x = r718ab(sensorData)
	case "17" :
		log.Println("R718CT2")
		x = r718ct2(sensorData)
	case "4a" :
		log.Println("R718N3")
		x = r718n3(sensorData)
	case "26" :
		log.Println("R718MA")
		x = r718ma(sensorData)
	case "03" :
		log.Println("RB11E")
		x = rb11e(sensorData)
	case "05" :
		log.Println("RA0715")
		x = ra0715(sensorData)
	case "35" :
		log.Println("RA0716")
		x = ra0716(sensorData)
	case "72" :
		log.Println("R718PC")
		x = r718pc(sensorData)
	case "44" :
		log.Println("R718KA2")
		x = r718ka2(sensorData)
	case "01":
		log.Println("R711")
		x = r711(sensorData)
	case "3f":
		log.Println("R718H2")
		x = r718h2(sensorData)
	case "49":
		log.Println("R718N17")
		x = r718n17(sensorData)
	case "36":
		log.Println("R72616A")
		x = r72616a(sensorData)
	case "1c":
		log.Println("R718E")
		x = r718e(sensorData)
	default:
		log.Println("default")
	}
	return x
}

//>============================================XXXXXXXXXXXX=============================================<



//>XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX<
//>=============================================< End Parse Netvox Sensor >=====================================================<
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
	sensorData, err := base64.StdEncoding.DecodeString(tempData)
	if err != nil {  
		log.Println("Falied to Perfrom URL Encoding", sensorData)  
		return  
	}
	//decoded message here [1 19 .....]
	//Recognize the sensor
	log.Println("Sensor Data ",parseSensorData(sensorData))
	payload := []byte(parseSensorData(sensorData))

	//Find a mechanism to send to the iMine app running on some port

	//log.Println("Message payload in string ",message)
	//log.Println("message payload ",payload)
	//log.Println("Type of message payload ",reflect.TypeOf(msg.Payload()))
	//TODO: do not publish data if unnecessary parameters like multipliers in case of current meter. Solve this using boolean type of variable. if true, publish data else don't
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

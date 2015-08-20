package main

import (
    "fmt"
    "flag"
    "os"
    "strings"
    "time"
    "github.com/Shopify/sarama"
    influx "github.com/influxdb/influxdb/client"
    sensory_proto "github.com/ekarlso/sensory-persister/proto"
    proto "github.com/golang/protobuf/proto"

    "net/url"
)

var (
    brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "Kafka brokers to use")
)

var typeToString = map[int32]string {
    1: "humidity",
    2: "temperature",
    3: "gas",
    4: "motion.microwave",
    5: "motion.infrared",
}

var senderToString = map[int32]string {
    1: "bedroom",
}

func main() {
    flag.Parse()

    if *brokers == "" {
        flag.PrintDefaults()
        os.Exit(1)
    }

    brokerList := strings.Split(*brokers, ",")

    cfg := sarama.NewConfig()

    kafkaCllient, e := sarama.NewClient(brokerList, cfg)
    if e != nil {
        panic(e)
    }

    consumer, e := sarama.NewConsumerFromClient(kafkaCllient)
    if e != nil {
        panic(e)
    }

    pConsumer, _ := consumer.ConsumePartition("sensors", 0, 0)

    u, _ := url.Parse("http://localhost:8086")
    influxConfig := influx.Config{
        URL: *u,
        Username: "bridge",
        Password: "bridge",
    }
    influxClient, _ := influx.NewClient(influxConfig)
    influxClient.Ping()

    for msg := range pConsumer.Messages() {
        // Decode the SensorData from protobuf string data.
        data := &sensory_proto.SensorData{}
        proto.Unmarshal(msg.Value, data)

        pts := make([]influx.Point, len(data.Reads))

        for i, read := range data.Reads {
            readType := typeToString[read.Type]

            var tm time.Time
            if read.Timestamp == 0 {
                tm = time.Now()
            } else {
                print(intVal)
                tm = time.Unix(int64(read.Timestamp), 0)
            }

            fields := make(map[string]interface{})
            var (
                intVal *int32 = &read.Intval
                fVal *float32 = &read.Floatval
            )
            if intVal != nil {
                fields["value"] = *intVal
            }
            if fVal != nil {
                fields["value"] = *fVal
            }
            if sender, ok := senderToString[data.SenderId]; ok {
                fields["sender"] = sender
            }

            point := influx.Point{
                Measurement: readType,
                Fields: fields,
                Tags: map[string]string {
                },
                Time: tm,
            }

            pts[i] = point
            fmt.Printf("Sender ID %d type(%s) int(%d) float(%f)\n", data.SenderId, readType, read.Intval, read.Floatval)
        }

        bps := influx.BatchPoints{
            Points: pts,
            Database: "sensors",
            RetentionPolicy: "default",
        }

        _, err := influxClient.Write(bps)
        if err != nil {
            print("ERROR")
            fmt.Println(err)
        }
    }
}

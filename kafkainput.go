package kafka

import (
        "fmt"
        "strings"
        "sync/atomic"
        "time"

        "gopkg.in/Shopify/sarama.v1"
        cluster "github.com/bsm/sarama-cluster"

        log "github.com/Sirupsen/logrus"
        "github.com/mozilla-services/heka/message"
        "github.com/mozilla-services/heka/pipeline"
)

var _ pipeline.ReportingPlugin = &CompatibleKafkaInput{}

type CompatibleKafkaInputConfig struct {
        Splitter string
        Brokers string `toml:"brokers"`
        Topics []string
        Group string
}

type CompatibleKafkaInput struct {
        processMessageCount    int64
        processMessageFailures int64

        config   *CompatibleKafkaInputConfig
        pConfig  *pipeline.PipelineConfig
        ir       pipeline.InputRunner
        stopChan chan bool
        name     string

        /////////////////////

        cg *cluster.Consumer
}

func (k *CompatibleKafkaInput) ConfigStruct() interface{} {
        return &CompatibleKafkaInputConfig{
                Splitter: "NullSplitter",
        }
}

func (k *CompatibleKafkaInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
        k.pConfig = pConfig
}

func (k *CompatibleKafkaInput) SetName(name string) {
        k.name = name
}

func (k *CompatibleKafkaInput) Init(kconfig interface{}) (err error) {
        k.config = kconfig.(*CompatibleKafkaInputConfig)
        if k.config.Brokers == "" {
                return fmt.Errorf("brokers nil")
        }
        if len(k.config.Topics) == 0 {
                return fmt.Errorf("kafka topics is nil")
        }
        if k.config.Group == "" {
                return fmt.Errorf("kafka consumer group is nil")
        }
        k.stopChan = make(chan bool, 1)
        config := cluster.NewConfig()
        config.Consumer.Offsets.Initial = sarama.OffsetOldest
        //config.Consumer.Offsets.Initial = sarama.OffsetNewest
        config.Consumer.Return.Errors = true
        config.Group.Return.Notifications = false
        //config.Group.Return.Notifications = true

        //sarama config
        config.Config.Consumer.Fetch.Default = 32768 //10240 //default 32768  32k
        //config.Config.Consumer.Fetch.Max = 0
        config.Config.ChannelBufferSize = 1024

        if log.GetLevel() == log.DebugLevel {
                //sarama.Logger = log.StandardLogger()
        }

        be := time.Now()
        k.cg, err = cluster.NewConsumer(strings.Split(k.config.Brokers, ","), k.config.Group, k.config.Topics, config)
        if err != nil {
                log.Errorf("init kafka consumer err:%v", err)
                return err
        }

        end := time.Now()
        log.Debugf("compatible kafka subcribes cost:%s  utc:%d", end.Sub(be).String(), end.UnixNano())

        return err
}

func (k *CompatibleKafkaInput) addField(pack *pipeline.PipelinePack, name string,
        value interface{}, representation string) {

        if field, err := message.NewField(name, value, representation); err == nil {
                pack.Message.AddField(field)
        } else {
                k.ir.LogError(fmt.Errorf("can't add '%s' field: %s", name, err.Error()))
        }
}

func (k *CompatibleKafkaInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) (err error) {
        sRunner := ir.NewSplitterRunner("")

        defer func() {
                k.cg.Close()
                sRunner.Done() // delete splitrunner from  allspliter
        }()
        k.ir = ir

        var (
                hostname = k.pConfig.Hostname()
                mesg     *sarama.ConsumerMessage
                n        int
        )

        packDec := func(pack *pipeline.PipelinePack) {
                pack.Message.SetType("uparse.kafka")
                pack.Message.SetLogger(k.name)
                pack.Message.SetHostname(hostname)
                k.addField(pack, "topic", mesg.Topic, "")
        }
        if !sRunner.UseMsgBytes() {
                sRunner.SetPackDecorator(packDec)
        }

        log.Debugf("compatible kafka inrunloop utc:%d", time.Now().UnixNano())
        myticker := time.NewTicker(5 * time.Second)
        var precount int64 = 0
        var nowcount int64 = 0
        //idledur := time.Duration(2 * time.Second)
        for {
                select {
                case ev, ok := <-k.cg.Messages():
                        if ok {
                                k.cg.MarkOffset(ev, "")
                                nowcount = atomic.AddInt64(&k.processMessageCount, 1)
                                mesg = ev
                                if n, err = sRunner.SplitBytes(ev.Value, nil); err != nil {
                                        atomic.AddInt64(&k.processMessageFailures, 1)
                                        ir.LogError(fmt.Errorf("processing message from topic %s err:%v cost:%s", ev.Topic, err, time.Since(kbg).String()))
                                        continue
                                }
                                if n > 0 && n != len(ev.Value) {
                                        atomic.AddInt64(&k.processMessageFailures, 1)
                                        ir.LogError(fmt.Errorf("extra data dropped in message from topic %s cost:%s", ev.Topic, time.Since(kbg).String()))
                                        continue
                                }

                case ntf, more := <-k.cg.Notifications():
                        if more {
                                log.Debugf("kafkaoutput Rebalanced: %+v", ntf)
                        }
                case err, more := <-k.cg.Errors():
                        if more {
                                log.Errorf("kafkaoutput Error: %s", err.Error())
                        }

                case <-k.stopChan:
                        ir.LogMessage("compatible kafka quit loop")
                        return nil
                case <-myticker.C:
                        log.Infof("kafkaconsumer consumer %s:%d msgs in 5 second", k.config.Topics[0], nowcount-precount)
                        precount = nowcount
                }
        }
}

func (k *CompatibleKafkaInput) Stop() {
        k.stopChan <- true
        close(k.stopChan)
}

func (k *CompatibleKafkaInput) ReportMsg(msg *message.Message) error {
        message.NewInt64Field(msg, "ProcessMessageCount",
                atomic.LoadInt64(&k.processMessageCount), "count")
        message.NewInt64Field(msg, "ProcessMessageFailures",
                atomic.LoadInt64(&k.processMessageFailures), "count")
        return nil
}

func (k *CompatibleKafkaInput) CleanupForRestart() {
        return
}

func init() {
        pipeline.RegisterPlugin("CompatibleKafkaInput", func() interface{} {
                return new(CompatibleKafkaInput)
        })
}

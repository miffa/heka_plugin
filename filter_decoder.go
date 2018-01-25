package plugins

import (
        "fmt"
        "sync/atomic"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
)

type FilterDecoderConfig struct {
        // Regular expression that describes log line format and capture group
        // values.
        // MatchRegex string `toml:"match_regex"`
        Type      string `toml:"router_type"`
        Matchtype string `toml:"filter_formula"` //http://hekad.readthedocs.io/en/v0.10.0/message_matcher.html
}

type FilterDecoder struct {
        Config  *FilterDecoderConfig
        dRunner DecoderRunner

        filter                    *message.MatcherSpecification
        processMessageCount       int64
        processMessageFilterCount int64
}

func (decoder *FilterDecoder) ConfigStruct() interface{} {
        return &FilterDecoderConfig{}
}

func (decoder *FilterDecoder) Init(config interface{}) (err error) {
        conf := config.(*FilterDecoderConfig)
        decoder.Config = conf

        if decoder.Config.Matchtype == "" {
                err = fmt.Errorf("Matchtype is empty in config")
                return
        }
        if decoder.filter, err = message.CreateMatcherSpecification(decoder.Config.Matchtype); err != nil {
                return
        }

        return
}

// Heka will call this to give us access to the runner.
func (decoder *FilterDecoder) SetDecoderRunner(dr DecoderRunner) {
        decoder.dRunner = dr
}

// Runs the message payload against decoder's regex. If there's a match, the
// message will be populated based on the decoder's message template, with
// capture values interpolated into the message template values.
func (decoder *FilterDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {

        pack.Message.SetType(decoder.Config.Type)
        atomic.AddInt64(&decoder.processMessageCount, 1)

        //be := time.Now()
        if decoder.filter.Match(pack.Message) {
                //decoder.dRunner.LogMessage(fmt.Sprintf("%s filter msg", decoder.Config.Matchtype))
                // ignore this packet
                pack.Recycle(nil)
                atomic.AddInt64(&decoder.processMessageFilterCount, 1)
                return
        } else {
                //decoder.dRunner.LogMessage(fmt.Sprintf("%s not filter msg", decoder.Config.Matchtype))
        }
        //costtime := time.Since(be).Nanoseconds()
        //decoder.dRunner.LogMessage(fmt.Sprintf("%s filter msg cost %d", decoder.Config.Matchtype, costtime))

        packs = []*PipelinePack{pack}
        return
}
func (d *FilterDecoder) Shutdown() {
        return
}

func (k *FilterDecoder) ReportMsg(msg *message.Message) error {
        message.NewInt64Field(msg, "ProcessMessageCount",
                atomic.LoadInt64(&k.processMessageCount), "count")
        message.NewInt64Field(msg, "ProcessMessageFilterCount",
                atomic.LoadInt64(&k.processMessageFilterCount), "count")
        return nil
}

func init() {
        RegisterPlugin("FilterDecoder", func() interface{} {
                return new(FilterDecoder)
        })
}

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"strings"
	"time"
	"log"
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topicIn = flag.String("topic-in", "GoRuleEngineServiceIn", "input topic")
	eosTopicOut = flag.String("eos-topic-out", "GoRuleEngineServiceOutTransformationServiceIn", "consume from topic-in, modify, and write to eos-topic-out")

	group = flag.String("group", "eos-task-force-group", "group to use for EOS consuming")

	consumeTxnID = flag.String("consume-txn-id", "eos-eos-consumer-12445543", "transactional ID to use for the EOS consumer/producer")

	debugHost = flag.String("debug-host", "127.0.0.1:9999", "curl http://host:port/metrics : prometheus metrics")

)

type jstruct struct {
    TransactionId   	string   `json:"transactionId"`
	TransactionStatus   string   `json:"transactionStatus"`
    TransactionTracking []string `json:"transactionTracking"`
	TransactionTimestamp string  `json:"transactionTimestamp"`
}

func my_now() string {
	t := time.Now().Local()

	return fmt.Sprintf("%d/%02d/%02d %02d:%02d:%02d",
        t.Year(), t.Month(), t.Day(),
        t.Hour(), t.Minute(), t.Second())
}

//
// produce and consume in the same transaction
//
func eos_prod_cons(ctx context.Context, metrics *Metrics, comm chan string) error {
	log.Println("[eos] [INFO] started ")

	sess, err := kgo.NewGroupTransactSession(
		
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*eosTopicOut),
		kgo.TransactionalID(*consumeTxnID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.WithHooks(metrics),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return  my_now() + " [eos] "
		})),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topicIn),
		kgo.RequireStableFetchOffsets(),
	)
	if err != nil {
		log.Printf("unable to create eos consumer/producer: %v", err)
	}
	defer sess.Close()

	for {
			fetches := sess.PollFetches(ctx)

			if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
				for _, fetchErr := range fetchErrs {
					log.Printf("[eos] [INFO] error consuming from topic: topic = %s, partition = %d, err = %v\n",
						fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
				}
				// The errors may be fatal for the partition (auth
				// problems), but we can still process any records if
				// there are any.
			}

			if err := sess.Begin(); err != nil {
				// Similar to above, we only encounter errors here if
				// we are not transactional or are already in a
				// transaction. We should not hit this error.
				log.Printf("[eos] [INFO] unable to start transaction: %v\n", err)
			}

			e := kgo.AbortingFirstErrPromise(sess.Client())
			fetches.EachRecord(func(r *kgo.Record) {
				// sync with rule engine
				select {
					case comm <- string(r.Value):
						log.Println("[eos] [INFO] sending data to rule engine")

						select {
						case msg := <- comm:
							log.Println("[eos] [INFO] receive from rule engine. Publish to kafka")
							sess.Produce(ctx, kgo.StringRecord(string(msg)), e.Promise())
					}
				}
				
			})
			
			committed, err := sess.End(ctx, e.Err() == nil)

			if committed {
				log.Println("[eos] [INFO] commit successful!")
			} else {
				// A failed End always means an error occurred, because
				// End retries as appropriate.
				log.Printf("[eos] [INFO] unable to eos commit: %v\n", err)
			}

			select {
				case <- ctx.Done():
					log.Println("[eos] [INFO] stopping ")
					return ctx.Err()
				default:
					// stay in the loop
			}	
		}
}

func rule_engine(ctx context.Context, comm chan string) error {
	log.Println("[rule engine] [INFO] started ")
	for {
		select {
		case kafka_message := <- comm:
			log.Println("[rule engine] [INFO] receive from kafka ")
			dat := jstruct{}

			if err := json.Unmarshal([]byte(kafka_message), &dat); err != nil {
				log.Printf("***JSON:%s****", err)
			}
			
			transactionId := dat.TransactionId
			// transactionStatus := dat.TransactionStatus
			transactionTracking := dat.TransactionTracking

			s := []string{"Rule Service"}

			map_msg := &jstruct{
				TransactionId: transactionId,
				TransactionStatus: "PENDING", 
				TransactionTracking: append(transactionTracking, s...),
				TransactionTimestamp: my_now(),
			}

			msg, err1 := json.Marshal(map_msg)

			if err1 != nil {
				log.Printf("***JSON-MARSHALL:%s****", err1)
			}

			comm <- string(msg)
			
		case <- ctx.Done():
			log.Println("[rule engine] [INFO] stopped ")
			return ctx.Err()
		}
	}
}

func main() {
	
	ctx, cancel := context.WithCancel(context.Background())

	metrics := NewMetrics("kgo")

	flag.Parse()

	if *topicIn == "" || *eosTopicOut == "" {
		log.Fatalf("missing either -topic-in (%s) or -eos-topic-out (%s)\n", *topicIn, *eosTopicOut)
	}

	comm := make(chan string)

	var wg sync.WaitGroup

	wg.Add(2)
	go func (ctx context.Context, comm chan string) {
		defer wg.Done()
		rule_engine(ctx, comm)
	}(ctx, comm)

	go func (ctx context.Context, metrics *Metrics, comm chan string) {
		defer wg.Done()
		eos_prod_cons(ctx, metrics, comm)
	}(ctx, metrics, comm)

	go func() {
		http.Handle("/metrics", metrics.Handler())
		log.Fatal(http.ListenAndServe(*debugHost, nil))
	}()

	shutdown_channel := make(chan os.Signal)
	signal.Notify(shutdown_channel, syscall.SIGINT, syscall.SIGTERM)
	s := <-shutdown_channel

	log.Println("Server received signal:", s)

	cancel()
	wg.Wait()	
}

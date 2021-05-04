package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"

	"github.com/aerospike/aerospike-client-go"
)

func main() {
	log.SetFlags(log.Lmicroseconds)

	asHost := flag.String("ashost", "127.0.0.1", "Aerospike host")
	asPort := flag.Int("asport", 3000, "Aerospike port")
	durable := flag.Bool("durable", false, "durable deletes")
	n := flag.Int("n", 100, "number of items to create and delete")

	flag.Parse()
	var policy *aerospike.WritePolicy
	if *durable {
		policy := aerospike.NewWritePolicy(0, 0)
		policy.DurableDelete = true
	}

	// define a client to connect to
	client, err := aerospike.NewClient(*asHost, *asPort)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("creating", *n, "items")
	for i := 0; i < 10**n; i++ {
		index := rand.Intn(*n - 1)
		key, err := aerospike.NewKey("bender", "amnontest", index)
		if err != nil {
			log.Fatalln(err)
		}
		err = client.PutBins(policy, key,
			aerospike.NewBin("seq", i))

		if err != nil {
			log.Fatalln(err)
		}

	}

	// delete the key, and check if key exists
	log.Println("deleting", *n, "items")
	for i := 0; i < *n; i++ {
		key, err := aerospike.NewKey("bender", "amnontest", i)
		if err != nil {
			log.Fatalln(err)
		}

		_, err = client.Delete(policy, key)
		if err != nil {
			log.Fatalln(err)
		}
	}

	fmt.Printf("done")
}

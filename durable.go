package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"

	"github.com/aerospike/aerospike-client-go"
)

func randbytes(buf []byte, n int) []byte {
	len := rand.Intn(n)
	for i := 0; i < len; i++ {
		buf[i] = 'a' + byte(rand.Intn(26))
	}
	return buf[:len]
}

func main() {
	log.SetFlags(log.Lmicroseconds)
	buf := make([]byte, 8001)
	asHost := flag.String("ashost", "127.0.0.1", "Aerospike host")
	asPort := flag.Int("p", 4000, "Aerospike port")
	durable := flag.Bool("durable", false, "durable deletes")
	delete := flag.Bool("delete", false, "do not create  - only delete")
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

	if !*delete {
		log.Println("creating", *n, "items")
		for i := 0; i < 10**n; i++ {
			index := rand.Intn(*n - 1)
			key, err := aerospike.NewKey("bender", "amnontest", index)
			if err != nil {
				log.Fatalln(err)
			}
			err = client.PutBins(policy, key,
				aerospike.NewBin("seq", randbytes(buf, 8000)))

			if err != nil {
				log.Fatalln(err)
			}

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

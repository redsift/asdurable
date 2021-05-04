package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aerospike/aerospike-client-go"
)

func main() {
	asHost := flag.String("ashost", "127.0.0.1", "Aerospike host")
	asPort := flag.Int("asport", 3000, "Aerospike port")
	durable := flag.Bool("durable", false, "durable deletes")
	n := flag.Int("n", 100, "number of items to create and delete")

	flag.Parse()
	policy := aerospike.NewWritePolicy(0, 0)
	if *durable {
		policy.DurableDelete = true

	}

	// define a client to connect to
	client, err := aerospike.NewClient(*asHost, *asPort)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("creating", *n, "items")
	for i := 0; i < *n; i++ {
		key, err := aerospike.NewKey("test", "amnontest", i)
		if err != nil {
			log.Fatalln(err)
		}
		err = client.PutBins(policy, key,
			aerospike.NewBin("seq", 123))

		if err != nil {
			log.Fatalln(err)
		}

	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("hit enter to continue> ")
	text, _ := reader.ReadString('\n')
	fmt.Println(text)

	// delete the key, and check if key exists
	log.Println("deleting", *n, "items")
	for i := 0; i < *n; i++ {
		key, err := aerospike.NewKey("test", "amnontest", i)
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

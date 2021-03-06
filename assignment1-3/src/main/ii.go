package main

// import "os"
// import "fmt"
// import "mapreduce"

import (
	"fmt"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	notLetters := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	valuesArr := strings.FieldsFunc(value, notLetters)

	for _, val := range valuesArr {
		// Exactly the same as assignment1-2, the only difference is that the
		// mapping will be done to the document, rather than using a count.
		res = append(res, mapreduce.KeyValue{val, document})
	}
	return
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge

	// It seems that the best way to create a set, is by using a map
	// structure and adding a value to each key.
	docsSet := make(map[string]bool)
	for _, doc := range values {
		docsSet[doc] = true
	}

	// Obtaining the not repeated elements
	var docs []string
	for doc := range docsSet {
		docs = append(docs, doc)
	}

	// Sorting the elements as requested in the task.
	sort.Strings(docs)

	// Preparing output string as requested as an output in Part III.
	numberOfDocs := strconv.Itoa(len(docs))
	result := numberOfDocs + " "
	for i, doc := range docs {
		if i > 0 {
			result = result + ","
		}
		result = result + doc
	}
	return result
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}

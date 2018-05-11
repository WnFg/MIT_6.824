package mapreduce

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	key_values := make(map[string][]string)
	var keys []string
	for i := 0; i < nMap; i++ {
		sourceFile := reduceName(jobName, i, reduceTask)
		inputFile, err := os.Open(sourceFile)
		if err != nil {
			return
		}
		defer inputFile.Close()

		reader := bufio.NewReader(inputFile)
		for {
			inputKey, readerError := reader.ReadString('\n')
			inputValue, readerError := reader.ReadString('\n')
			if readerError == io.EOF {
				break
			}
			inputKey = inputKey[:len(inputKey)-1]
			inputValue = inputValue[:len(inputValue)-1]
			_, ok := key_values[inputKey]
			if ok == false {
				//			key_values[inputKey] = append(key_values[inputKey], inputValue)
				keys = append(keys, inputKey)
			}
			key_values[inputKey] = append(key_values[inputKey], inputValue)
		}
	}

	sort.Strings(keys)

	oFile, oerr := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0666)
	//oDebug, oerr := os.OpenFile(outFile + "-debug", os.O_WRONLY|os.O_CREATE, 0666)
	if oerr != nil {
		return
	}
	defer oFile.Close()
	//defer oDebug.Close()
	enc := json.NewEncoder(oFile)
	//encDebug := json.NewEncoder(oDebug)
	//encDebug.Encode(key_values)

	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, key_values[key])})
	}
}

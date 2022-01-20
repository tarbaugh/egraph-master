package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
)

func newClient(serverAddr string) *dgo.Dgraph {
	// Dial a gRPC connection
	d, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

func setup(c *dgo.Dgraph) {
	// Current schema for eCAR v0.3
	err := c.Alter(context.Background(), &api.Operation{
		Schema: `
            action: [uid] .
            actorID: string @index(hash) .
            acts_on: [uid] .
            dgraph.graphql.schema: string .
            object: string @index(exact) .
            objectID: string @index(hash) .
            action_type: string @index(exact) .
            hostname: string @index(exact) .
            ppid: string @index(exact) .
            timestamp: string .
            pid: string @index(exact) .
            id: string @index(hash) .
        `,
	})
	if err != nil {
		log.Fatal(err)
	}

}

func dropAll(c *dgo.Dgraph) {
	err := c.Alter(context.Background(), &api.Operation{DropOp: api.Operation_ALL})
	if err != nil {
		log.Fatal(err)
	}
}

func writeFile(filename string, serverAddr string, zeroAddr string, only string) {
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("Open read-file error: %v", err)
		return
	}

	os.OpenFile(filename+".txt", os.O_CREATE, os.ModePerm)
	g, err := os.OpenFile(filename+".txt", os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("Open write-file error: %v", err)
		return
	}

	var pred string
	var search string
	var pval string
	if only != "" {
		var count int
		for i := 0; string(only[i]) != ":"; i++ {
			count++
		}
		pred = string(only[:count])
		last_count := int(count)
		for i := count + 1; string(only[i]) != ":"; i++ {
			count++
		}
		search = string(only[last_count:count])
		p_count := count + 1
		pval = string(only[p_count:])
	}
	sc := bufio.NewScanner(f)
	aIDs := make(map[string]bool)
	oIDs := make(map[string]bool)
	var actorID string
	var objectID string
	if only != "" {
		for sc.Scan() {
			line := sc.Text()
			var data map[string]interface{}
			err := json.Unmarshal([]byte(line), &data)
			if err != nil {
				panic(err)
			}
			if verify(pred, search, pval, data) == true {
				actorID = data["actorID"].(string)
				objectID = data["objectID"].(string)
				if aIDs[actorID] == true && oIDs[objectID] == true {
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					g.WriteString(actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp)
				} else if aIDs[actorID] == true {
					oIDs[objectID] = true
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					oID := fmt.Sprintf("_:%v <objectID> %q .\n", data["objectID"], data["objectID"])
					g.WriteString(actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp + oID)
				} else if oIDs[objectID] == true {
					aIDs[actorID] = true
					aID := fmt.Sprintf("_:%v <actorID> %q .\n", actorID, actorID)
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					g.WriteString(aID + actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp)
				} else {
					aIDs[actorID] = true
					oIDs[objectID] = true
					aID := fmt.Sprintf("_:%v <actorID> %q .\n", actorID, actorID)
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					oID := fmt.Sprintf("_:%v <objectID> %q .\n", data["objectID"], data["objectID"])
					g.WriteString(aID + actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp + oID)
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("Write file error: %v", err)
		return
	}
	f.Close()
	g.Close()
}

func writeFilegz(filename string, serverAddr string, zeroAddr string, only string) {
	f, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("Open read-file error: %v", err)
		return
	}

	fgz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("Open gzipped-read-file error: %v", err)
	}

	os.OpenFile(filename+".txt", os.O_CREATE, os.ModePerm)
	g, err := os.OpenFile(filename+".txt", os.O_WRONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("Open write-file error: %v", err)
		return
	}

	var pred string
	var search string
	var pval string
	if only != "" {
		var count int
		for i := 0; string(only[i]) != ":"; i++ {
			count++
		}
		pred = string(only[:count])
		last_count := int(count)
		for i := count + 1; string(only[i]) != ":"; i++ {
			count++
		}
		search = string(only[last_count:count])
		p_count := count + 1
		pval = string(only[p_count:])
	}

	sc := bufio.NewScanner(fgz)
	aIDs := make(map[string]bool)
	oIDs := make(map[string]bool)
	var actorID string
	var objectID string
	if only != "" {
		for sc.Scan() {
			line := sc.Text()
			var data map[string]interface{}
			err := json.Unmarshal([]byte(line), &data)
			if err != nil {
				panic(err)
			}
			if verify(pred, search, pval, data) == true {
				actorID = data["actorID"].(string)
				objectID = data["objectID"].(string)
				if aIDs[actorID] == true && oIDs[objectID] == true {
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					g.WriteString(actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp)
				} else if aIDs[actorID] == true {
					oIDs[objectID] = true
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					oID := fmt.Sprintf("_:%v <objectID> %q .\n", data["objectID"], data["objectID"])
					g.WriteString(actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp + oID)
				} else if oIDs[objectID] == true {
					aIDs[actorID] = true
					aID := fmt.Sprintf("_:%v <actorID> %q .\n", actorID, actorID)
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					g.WriteString(aID + actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp)
				} else {
					aIDs[actorID] = true
					oIDs[objectID] = true
					aID := fmt.Sprintf("_:%v <actorID> %q .\n", actorID, actorID)
					actionedge := fmt.Sprintf("_:%v <action> _:%v .\n", actorID, data["id"])
					action := fmt.Sprintf("_:%v <action_type> %q .\n", data["id"], data["action"])
					hostname := fmt.Sprintf("_:%v <hostname> %q .\n", data["id"], data["hostname"])
					id := fmt.Sprintf("_:%v <id> %q .\n", data["id"], data["id"])
					pid := fmt.Sprintf("_:%v <pid> \"%v\" .\n", data["id"], data["pid"])
					ppid := fmt.Sprintf("_:%v <ppid> \"%v\" .\n", data["id"], data["ppid"])
					timestamp := fmt.Sprintf("_:%v <timestamp> %q .\n", data["id"], data["timestamp"])
					objectedge := fmt.Sprintf("_:%v <acts_on> _:%v .\n", data["id"], data["objectID"])
					object := fmt.Sprintf("_:%v <object> %q .\n", data["objectID"], data["object"])
					oID := fmt.Sprintf("_:%v <objectID> %q .\n", data["objectID"], data["objectID"])
					g.WriteString(aID + actionedge + action + hostname + objectedge + id + object + pid + ppid + timestamp + oID)
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("Write file error: %v", err)
		return
	}
	f.Close()
	g.Close()
	fgz.Close()
}

func verify(pred string, search string, pval string, data map[string]interface{}) bool {
	if data[pred].(string) != pval {
		return false
	}
	return true
}

func loadFile(filename string, serverAddr string, zeroAddr string) {
	srvr := fmt.Sprintf("--alpha=%v", serverAddr)
	zero := fmt.Sprintf("--zero=%v", zeroAddr)
	cmd := exec.Command("dgraph", "live", "-f", filename+".txt", "--format=rdf", "--xidmap=xid_uid", srvr, zero)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		m := scanner.Text()
		fmt.Println(m)
		log.Printf(m)
	}
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
}
func addData(filepath string, serverAddr string, zeroAddr string, gz bool, writeOnlyPts string) {
	count := len(filepath)
	for i := 0; string(filepath[len(filepath)-(i+1)]) != "/"; i++ {
		count -= 1
	}
	filename := string(filepath[count:])
	if gz == false {
		writeFile(filename, serverAddr, zeroAddr, writeOnlyPts)
		loadFile(filename, serverAddr, zeroAddr)
	} else {
		writeFilegz(filename, serverAddr, zeroAddr, writeOnlyPts)
		loadFile(filename, serverAddr, zeroAddr)
	}
}

func main() {
	watchPtr := flag.Bool("watch", false, "Would you like to watch a directory?")
	zeroAddrPtr := flag.String("zero", "localhost:6080", "Address of dGraph Zero")
	writePtr := flag.String("write", "", "Specify the file which you would like to re-write in RDF-triples")
	writeOnlyPts := flag.String("only", "", "For a predicate how would you like to search and for what value? \"-only=predicate:search:value\"")
	loadPtr := flag.String("load", "", "Specify the already processed file you would like to load via Dgraph live")
	dirPtr := flag.String("dir", "", "Specify the directory you would like to watch (ignore for current)")
	serverAddrPtr := flag.String("url", "localhost:9080", "Address of dGraph to gRPC dial")
	dropPtr := flag.Bool("drop", false, "Would you like dGraph to drop all data?")
	setupPtr := flag.Bool("setup", false, "Would you like to add the eCAR schema?")
	flag.Parse()

	if *writePtr != "" {
		if strings.HasSuffix(*writePtr, ".json") {
			fmt.Println("Writing .json File: ", *writePtr, *writeOnlyPts)
			writeFile(*writePtr, *serverAddrPtr, *zeroAddrPtr, *writeOnlyPts)
		} else if strings.HasSuffix(*writePtr, ".json.gz") {
			fmt.Println("Writing .json.gz File: ", *writePtr, *writeOnlyPts)
			writeFilegz(*writePtr, *serverAddrPtr, *zeroAddrPtr, *writeOnlyPts)
		}
	}

	if *dropPtr != false || *setupPtr != false || *loadPtr != "" || *watchPtr != false {
		client := newClient(*serverAddrPtr)

		if *dropPtr == true {
			dropAll(client)
		}
		if *setupPtr == true {
			setup(client)
		}
		if *loadPtr != "" {
			loadFile(*loadPtr, *serverAddrPtr, *zeroAddrPtr)
		}

		if *dirPtr != "" && *watchPtr == false {
			log.Fatal("Must use \"-watch=true\" with \"-dir\"")
		}

		if *watchPtr == true {
			watcher, err := fsnotify.NewWatcher() // Downside of watching is no NFS format support
			if err != nil {
				log.Fatal(err)
			}
			defer watcher.Close()
			done := make(chan bool)
			go func() {
				for {
					select {
					case event, ok := <-watcher.Events:
						if !ok {
							return
						}
						if op := event.Op.String(); op == "CREATE" && strings.HasSuffix(event.Name, ".json") {
							fmt.Println("Adding .json File: ", event.Name)
							addData(event.Name, *serverAddrPtr, *zeroAddrPtr, false, *writeOnlyPts)
						} else if op == "CREATE" && strings.HasSuffix(event.Name, ".json.gz") {
							fmt.Println("Adding .json.gz File: ", event.Name)
							addData(event.Name, *serverAddrPtr, *zeroAddrPtr, true, *writeOnlyPts)
						}
					case err, ok := <-watcher.Errors:
						if !ok {
							return
						}
						log.Println("error:", err)
					}
				}
			}()

			path := *dirPtr
			if path == "" {
				dir, err := os.Getwd()
				if err != nil {
					log.Fatal(err)
				}
				err = watcher.Add(dir)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				os.Chdir(path)
				err = watcher.Add(path)
				if err != nil {
					log.Fatal(err)
				}
			}
			<-done
		}
	}
}

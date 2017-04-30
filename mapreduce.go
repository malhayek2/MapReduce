package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode"
)

type Datatype struct {
	key string
	val string
}

// func main() {
// 	//Pick some number of map tasks (say, m=9) and reduce tasks (say, r=3)
// 	testupndow()

// }
func testsplit() {
	// testsplit()
	testupndow()
	// go do()
	// dowload("http://localhost:8080/data", "./astaxie.pdf")
	// db, err := openDatabase("austen.sqlite3")
	// if err != nil {
	// 	log.Fatalf("openData func err: %s", err)
	// }
	// log.Println("db has been opened")
	// defer db2.Close()
	n, err := splitDatabase("austen", "output%d.sqlite3", 3)
	checkErr(err)
	log.Println(n)
	// name := fmt.Sprintf("output-%d.sqlite3" )
}
func testgather() {
	db2, err := openDatabase("output0.sqlite3")
	checkErr(err)
	gatherInto(db2, "output1.sqlite3")
	gatherInto(db2, "output2.sqlite3")

}

// url = "http://localhost:8080/data"
func testupndow() {
	port := "8080"
	// go runserver(getLocalAddress(), port)
	url := fmt.Sprintf("http://localhost:%s/data", port)
	// uri := fmt.Sprintf("http://localhost:%s/data/", port)
	// uri := fmt.Sprintf("http://")
	// upload("http://localhost:8080/data", "output1.sqlite3")
	upload(url, "output1.sqlite3")
	// upload(url,"output2.sqlite3")
	// download(url,"output1.sqlite3")
	// download(url,"output1.sqlite3")
	// download(url,"output2.sqlite3")

}
func runserver(url string, port string) {
	str := fmt.Sprintf("%s:%s", getLocalAddress(), port)
	fmt.Println(str)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("/tmp/data"))))
	if err := http.ListenAndServe(str, nil); err != nil {
		log.Printf("Error in HTTP server for %s: %v", str, err)
	}

}
func getLocalAddress() string {
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {
			addrs, err := elt.Addrs()
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}

	return localaddress
}

// opens db
// turn off journaling by doing 2 codes.
// on error db must close
func openDatabase(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatalf("error opening DB (%s)", err)
		db.Close()
		return nil, err
	}
	// defer db.Close()
	_, err = db.Exec("pragma synchronous = off")
	checkErr(err)
	_, err = db.Exec("pragma journal_mode = off")
	checkErr(err)
	return db, nil
}
func createDatabase(path string) (*sql.DB, error) {
	// if it exists !
	db1, err := sql.Open("sqlite3", path)
	if err == nil {
		// db1.Close()
		db1.Close()
		os.Remove(path)
		// log.Println("files has been removed")
	}
	db, err := openDatabase(path)
	checkErr(err)
	_, err = db.Exec("create table pairs (key text, value text)")
	checkErr(err)
	return db, nil
}
func splitDatabase(path string, outputPattern string, filecount int) ([]string, error) {
	//
	db, err := openDatabase(path)
	_, err = db.Exec("pragma journal_mode = off")
	checkErr(err)
	// safety
	// 	_, err = db.Exec(`UPDATE pairs SET value = REPLACE (value, '"', '')`)
	// if err != nil {
	// 	panic(err)
	// }
	res, err := db.Query("select count(1) from pairs")
	if err != nil {
		panic(err)
	}
	var n int
	for res.Next() {
		err = res.Scan(&n)
		log.Println(n)
	}
	if filecount > n {
		log.Fatalf(" filecount > n ")
	}
	lns := ((n) / filecount) + 1
	rows, err := db.Query("select key, value from pairs")
	checkErr(err)
	var currentline int = 0
	var currentslice int = 0
	var result []string
	var paths []string
	for rows.Next() {
		if currentline < n {
			item := Datatype{}
			err2 := rows.Scan(&item.key, &item.val)
			// log.Println(item.key)
			str := fmt.Sprintf(`INSERT INTO pairs (key, value) values("%s", "%s")`, item.key, item.val)
			if err2 == nil {
				currentline = currentline + 1
			}
			if err2 != nil {
				log.Fatalf("rows are out")
			}
			result = append(result, str)
		}
	}
	var x int = 0
	// shoud be like outputPattern = "output%d.sqlite3"
	for x < filecount {
		name := fmt.Sprintf(outputPattern, currentslice)
		paths = append(paths, name)
		log.Println(fmt.Sprintf("%s has been sliced", name))
		tmpdb, err := createDatabase(name)
		checkErr(err)
		var y int = (currentslice * lns)
		var i int = 0
		for i < lns {
			y := (i + y)
			if y < len(result) {
				qury := result[y]
				_, err = tmpdb.Exec(qury)
			}
			checkErr(err)
			i++
		}
		tmpdb.Close()
		currentslice++
		x++
	}
	return paths, nil
}
func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error){

// }
// url = "http://localhost:8080/data"
// file should be next to this file
// filename ="doc.pdf"
func download(uri string, filename string) error {
	str := fmt.Sprintf("%s/%s", uri, filename)
	fmt.Println("Downloading file...")
	// rawURL := "https://d1ohg4ss876yi2.cloudfront.net/golang-resize-image/big.jpg"
	fmt.Println(str)
	rawURL := str
	fileURL, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	path := fileURL.Path
	segments := strings.Split(path, "/")
	v := len(segments)
	fileName := segments[v-1] // change the number to accommodate changes to the url.Path position
	// strr := fmt.Sprintf("%s", filename)
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer file.Close()
	check := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}
	resp, err := check.Get(rawURL) // add a filter to check redirect
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer resp.Body.Close()
	// fmt.Println(resp.Status)
	size, err := io.Copy(file, resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s with %v bytes downloaded \n", fileName, size)
	return nil
}

// url = "http://localhost:8080/data"
// file should be next to this file
// filename ="doc.pdf"
func upload(uri string, filename string) {
	// str:= fmt.Sprintf("./%s", filename)
	//
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// newURL := fmt.Sprintf("%s")
	res, err := http.Post(uri, "sqlite3", file)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	fmt.Println(res)
	message, _ := ioutil.ReadAll(res.Body)
	fmt.Printf(string(message))
}

func gatherInto(db *sql.DB, filename string) error {
	// if filename is "blah.db"
	// segments := strings.Split(filename, ".")
	// newfilename := segments[0]
	str := fmt.Sprintf("attach `%s` as merge;\ninsert into pairs select * from merge.pairs;\ndetach merge;", filename)
	_, err := db.Exec(str)
	checkErr(err)
	return nil
}

//// WORKER API

// generated by the master and used to track a task on the master side and the worker side.
type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}
type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

// database feedback into.
type Pair struct {
	Key   string
	Value string
}

func newPair(key string, val string) *Pair {
	return &Pair{
		Key:   key,
		Value: val,
	}

}

// a client job must implement to interface
//client code will refer to this type as mapreduce.Interface ,
type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

// helper functions, genrate filenames
func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.sqlite3", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.sqlite3", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.sqlite3", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.sqlite3", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.sqlite3", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.sqlite3", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.sqlite3", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

//
func (task *MapTask) Process(tempdir string, client Interface) error {
	// tempdir = www.//localhost:8080/data/output.db
	urlPart := strings.Split(tempdir, "/")
	v := len(urlPart)
	filename := urlPart[v-1] // filename = "output.db"
	url := ""
	for i := 0; i <= v-2; i++ {
		url = fmt.Sprintf("%s%s/", url, urlPart[i])
	}
	download(url, filename)
	// open file
	db, err := openDatabase(filename)
	if err != nil {
		log.Fatalf("openData func err: %s", err)
	}

	// number of pairs process
	//
	str := fmt.Sprintf("SELECT * FROM pairs ORDER BY key;")
	_, err = db.Exec(str)
	checkErr(err)
	// get pairs
	rows, err := db.Query("select key, value from pairs")
	checkErr(err)
	dbPairs := make(chan Pair, 0)
	for rows.Next() {
		item := Pair{}
		err2 := rows.Scan(&item.Key, &item.Value)
		if err2 != nil {
			log.Fatalf("rows are out")
		}
		dbPairs <- item
		client.Map(item.Key, item.Value, dbPairs)
	}
	// save changes to db
	db.Close()
	// upload all files
	outputPattern := "output%d.sqlite3"
	_, err = splitDatabase(filename, outputPattern, 4)
	checkErr(err)
	urlup := fmt.Sprintf("%s/data/", url)
	for i := 0; i < 4; i++ {
		qurl := fmt.Sprintf(outputPattern, i)
		upload(urlup, qurl)
	}
	// dbPairs chan
	// client.Map(key, value string, output chan<- Pair)
	//
	return nil
}
func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// task.M is total tasks reduce N
	// tempdir is www.//localhost:8080/data/output.db and sourcehost[i]
	// pulls links for task and dowloads them all.
	var filename string
	for i := 0; i <= len(task.SourceHosts); i++ {
		url := task.SourceHosts[i]
		urlPart := strings.Split(url, "/")
		v := len(urlPart)
		filename = urlPart[v-1] // filename = "output.db"
		url2 := ""
		for i := 0; i <= v-2; i++ {
			url2 = fmt.Sprintf("%s%s/", url, urlPart[i])
		}
		download(url2, filename)
		db, err := createDatabase("all.db")
		checkErr(err)
		// gathers all files to new created DB ALL.DB.
		gatherInto(db, filename)
	}
	db, err := openDatabase(filename)
	if err != nil {
		log.Fatalf("openData func err: %s", err)
	}
	rows, err := db.Query("select key, value from pairs order by key, value")
	checkErr(err)
	keycount, valuecount, outcount := 0, 0, 0
	prev := ""
	var clientSucceeded chan bool
	var clientOutputSucceeded chan bool
	var clientValues chan string
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("err %v", err)
			return err
		}
		// close out the old client
		if key != prev && keycount > 0 {
			close(clientValues)
			clientValues = nil
			if !<-clientSucceeded {
				err := fmt.Errorf("reducer error")
				log.Printf("%v", err)
				return err
			}
			if !<-clientOutputSucceeded {
				clientOutputSucceeded = nil
			}
			// new client worker
			if key != prev || keycount == 0 {
				prev = key
				keycount++
				clientSucceeded = make(chan bool, 1)
				clientOutputSucceeded = make(chan bool, 1)
				clientValues = make(chan string, 1)
				clientOutput := make(chan Pair, 1)
				// new reducer
				go func(key string, values chan string, output chan Pair, success chan bool) {
					err := client.Reduce(key, values, output)
					if err != nil {
						log.Printf("reducer err")
					}
					success <- err == nil
				}(key, clientValues, clientOutput, clientSucceeded)
				// gather OUTPUT
				go func(output chan Pair, success chan bool) {
					for pair := range output {
						go func(key string, values chan string, output chan Pair, success chan bool) {
							err := client.Reduce(key, values, output)
							if err != nil {
								log.Printf("reducer err")
							}
							success <- err == nil
						}(key, clientValues, clientOutput, clientSucceeded)
						str := fmt.Sprintf(`INSERT INTO pairs (key, value) values("%s", "%s")`, pair.Key, pair.Value)
						if _, err := db.Exec(str); err != nil {
							success <- false
							return
						}
						outcount++
					}
					success <- true
				}(clientOutput, clientOutputSucceeded)

			}
			clientValues <- value
			valuecount++
		}
		if err := rows.Err(); err != nil {
			log.Printf("db error while reduce pair %v", err)
			return err
		}
		if !<-clientSucceeded {
			clientSucceeded = nil
		}
		close(clientValues)

	}
	return nil
}

/// CLient !

func main() {
	var c Client
	if err := Start(c); err != nil {
		log.Fatalf("%v", err)
	}
}
func (c Client) Start() error{
	// COMMAND LINES
	// Q master or worker?
	// PORT:
	//if worker then just run server.. and wait
	// enter filename optional
	// split file -- master host server to serve workers
	// genrate a map tasks and map reduce 
	// start a client rpc/http server to handle client requests // 
	// serve tasks to map tasks that request job or idle wait til done then reduce task it
	// wait all complete
	// gather to one file.
	// QUIT.
}

// Map and Reduce functions for a basic wordcount client

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

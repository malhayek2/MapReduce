package main

import (
	"bytes"
	"database/sql"
	"net/url"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type Datatype struct {
	key string
	val string
}



func main() {
	// testupndow()
	// testsplit()
	// go do()

	// dowload("http://localhost:8080/data", "./astaxie.pdf")
}
func testsplit() {
	db, err := openDatabase("austen.sqlite3")
	if err != nil {
		log.Fatalf("openData func err: %s", err)
	}
	log.Println("db has been opened", db)
	// defer db2.Close()
	n, err := splitDatabase("austen", "wat", 3)
	log.Println(n)
	// name := fmt.Sprintf("output-%d.sqlite3" )
	db2, err := openDatabase("output0")
	checkErr(err)
	gatherInto(db2, "output1")
}

// url = "http://localhost:8080/data"
func testupndow(){
	port := "8080"
	go runserver(getLocalAddress(), port)
	url := fmt.Sprintf("http://%s:%s/data",getLocalAddress(), port)
	filename := "doc.pdf"
	upload(url,filename)
	download(url,filename)
}
func runserver(url string, port string){
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
	// n number of pairs
	// var n int
	// output-%d.sqlite3
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
		name := fmt.Sprintf("output%d", currentslice)
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
func download(uri string, filename string) error{
		 str := fmt.Sprintf("%s/%s", uri, filename)
	     fmt.Println("Downloading file...")
         rawURL := str
         fileURL, err := url.Parse(rawURL)
         if err != nil {
                 panic(err)
         }
         path := fileURL.Path
         segments := strings.Split(path, "/")
         fileName := segments[2] // change the number to accommodate changes to the url.Path position
         strr := fmt.Sprintf("/tmp/%s", filename) 
         file, err := os.Create(strr)
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
         fmt.Printf("%s with %v bytes downloaded", fileName, size)
         return nil
}

// url = "http://localhost:8080/data"
// file should be next to this file
// filename ="doc.pdf"
func upload(url string, filename string) error { 
	extraParams := map[string]string{
		"title":       "My Document",
		"author":      "Myself and I",
		"description": "A document with all the Go programming language secrets",
	}
	
	request, err := newfileUploadRequest(url, extraParams, "file", filename)
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		log.Fatal(err)
	} else {
		body := &bytes.Buffer{}
		_, err := body.ReadFrom(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		resp.Body.Close()
		fmt.Println(resp.StatusCode)
		fmt.Println(resp.Header)
		fmt.Println(body)
	
	}
	return nil
}
// upload helper function
func newfileUploadRequest(uri string, params map[string]string, paramName string, path string) (*http.Request, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile(paramName, filepath.Base(path))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part, file)

	for key, val := range params {
		_ = writer.WriteField(key, val)
	}
	err = writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", uri, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, err
}
func gatherInto(db *sql.DB, filename string) error{
		// if filename is "blah.db"
	 	// segments := strings.Split(filename, ".")
   		// newfilename := segments[0]
	 	str := fmt.Sprintf("attach %s as merge;\ninsert into pairs select * from merge.pairs;\ndetach merge;", filename)
		_, err := db.Exec(str)
		checkErr(err)
		return nil
}





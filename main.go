package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	var file, output, url string

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "file",
				Value:       "file.parquet",
				Aliases:     []string{"f"},
				Usage:       "path of the parquet file",
				Destination: &file,
			},
			&cli.StringFlag{
				Name:        "url",
				Aliases:     []string{"u"},
				Usage:       "get parquet file from url",
				Destination: &url,
			},
			&cli.StringFlag{
				Name:        "output",
				Value:       "file",
				Aliases:     []string{"o"},
				Usage:       "output name of json file",
				Destination: &output,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "readcolumns",
				Usage: "add a task to the list",
				Action: func(c *cli.Context) error {
					if url != "" {
						saveFile(url)
						file = "dowloaded.parquet"
					}
					readcolumns(file)
					return nil
				},
			},
			{
				Name:  "tojson",
				Usage: "convert parquet file to json",
				Action: func(c *cli.Context) error {
					if url != "" {
						saveFile(url)
						file = "dowloaded.parquet"
					}
					toJSON(file, output)
					fmt.Println("JSON file created successfully", c.Args().First())
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func toJSON(path, output string) {
	var num int
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num = int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	file, _ := json.MarshalIndent(res, "", " ")

	err = ioutil.WriteFile(output+".json", file, 0644)

	if err != nil {
		log.Println("Can't Write the file", err)
		return
	}
}

func saveFile(url string) {
	fileURL := url
	if err := DownloadFile("dowloaded.parquet", fileURL); err != nil {
		panic(err)
	}
}

func DownloadFile(filepath string, url string) error {

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func readcolumns(path string) {

	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	res, err := pr.ReadByNumber(2)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	file, _ := json.MarshalIndent(res, "", " ")

	var result []interface{}
	json.Unmarshal([]byte(file), &result)
	file2, _ := json.Marshal(result[0])
	var result2 map[string]interface{}
	json.Unmarshal(file2, &result2)
	for key := range result2 {
		fmt.Println(key)
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	var file, output string

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
					readcolumns(file)
					return nil
				},
			},
			{
				Name:  "tojson",
				Usage: "convert parquet file to json",
				Action: func(c *cli.Context) error {
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

package main

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"flag"
)

type PosixInfo struct {
	FilePath string    `json:"file_path"`
	INode    uint64    `json:"inode"`
	Size     int64     `json:"size"`
	UID      uint32    `json:"uid"`
	GID      uint32    `json:"gid"`
	MTime    time.Time `json:"mtime"`
	CTime    time.Time `json:"ctime"`
}

type PosixCrawler struct {
	SubDirs   chan string
	Outputs   chan *PosixInfo
	wg        sync.WaitGroup
	concLimit chan bool
	pattern   *regexp.Regexp
}

func NewPosixCrawler(conc int, pattern string) *PosixCrawler {
	crawler := &PosixCrawler{
		SubDirs:   make(chan string, 4096),
		Outputs:   make(chan *PosixInfo, 4096),
		wg:        sync.WaitGroup{},
		concLimit: make(chan bool, conc),
	}

	if len(strings.TrimSpace(pattern)) > 0 {
		crawler.pattern = regexp.MustCompile(pattern)
	}

	return crawler
}

func readDir(path string) ([]os.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	list, err := f.Readdir(-1)
	f.Close()
	return list, err
}

func (pc *PosixCrawler) outputResult() {
	for info := range pc.Outputs {
		out, _ := json.Marshal(info)
		log.Printf("%v", string(out))
	}
}

func (pc *PosixCrawler) crawlDir(currPath string) {
	defer pc.wg.Done()
	defer func() { <-pc.concLimit }()
	files, err := readDir(currPath)
	if err != nil {
		return
	}

	for _, fi := range files {
		fileName := fi.Name()
		filePath := path.Join(currPath, fileName)

		if fi.IsDir() {
			pc.wg.Add(1)
			go func(p string) {
				pc.concLimit <- false
				pc.crawlDir(p)
			}(filePath)
			continue
		}

		if pc.pattern != nil && !pc.pattern.MatchString(filePath) {
			continue
		}

		stat := fi.Sys().(*syscall.Stat_t)
		info := &PosixInfo{
			FilePath: filePath,
			INode:    stat.Ino,
			Size:     stat.Size,
			UID:      stat.Uid,
			GID:      stat.Gid,
			MTime:    time.Unix(int64(stat.Mtim.Sec), int64(stat.Mtim.Nsec)).UTC(),
			CTime:    time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)).UTC(),
		}

		pc.Outputs <- info
	}

}

func (pc *PosixCrawler) Crawl(currPath string) {
	go pc.outputResult()

	pc.wg.Add(1)
	pc.concLimit <- false
	pc.crawlDir(currPath)
	pc.wg.Wait()
	close(pc.Outputs)

	pc.outputResult()
}

func main() {
	if len(os.Args) < 2 {
		panic("please specify root directory to crawl")
	}

	rootDir := os.Args[1]
	var pattern string

	if len(os.Args) > 2 {
		flagSet := flag.NewFlagSet("Usage", flag.ExitOnError)
		flagSet.StringVar(&pattern, "regexp", "", "Crawl regexp match")

		flagSet.Parse(os.Args[2:])
	}
	crawler := NewPosixCrawler(2, pattern)
	crawler.Crawl(rootDir)
}

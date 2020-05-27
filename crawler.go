package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
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
	SubDirs       chan string
	Outputs       chan *PosixInfo
	Error         chan error
	wg            sync.WaitGroup
	concLimit     chan bool
	pattern       *regexp.Regexp
	followSymlink bool
}

func NewPosixCrawler(conc int, pattern string, followSymlink bool) *PosixCrawler {
	crawler := &PosixCrawler{
		SubDirs:       make(chan string, 4096),
		Outputs:       make(chan *PosixInfo, 4096),
		Error:         make(chan error, 100),
		wg:            sync.WaitGroup{},
		concLimit:     make(chan bool, conc),
		followSymlink: followSymlink,
	}

	if len(strings.TrimSpace(pattern)) > 0 {
		crawler.pattern = regexp.MustCompile(pattern)
	}

	return crawler
}

func (pc *PosixCrawler) Crawl(currPath string) error {
	go pc.outputResult()

	pc.wg.Add(1)
	pc.concLimit <- false
	pc.crawlDir(currPath)
	pc.wg.Wait()

	close(pc.Outputs)
	pc.outputResult()

	close(pc.Error)
	var errors []string
	for err := range pc.Error {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf(strings.Join(errors, "\n"))
	}

	return nil
}

func (pc *PosixCrawler) crawlDir(currPath string) {
	defer pc.wg.Done()
	defer func() { <-pc.concLimit }()
	files, err := readDir(currPath)
	if err != nil {
		select {
		case pc.Error <- err:
		default:
		}
		return
	}

	for _, fi := range files {
		fileName := fi.Name()
		filePath := path.Join(currPath, fileName)
		fileMode := fi.Mode()

		if pc.followSymlink && (fileMode&os.ModeSymlink == os.ModeSymlink) {
			newFi, newPath, err := pc.resolveSymlink(currPath, fileName)
			if err != nil {
				select {
				case pc.Error <- err:
				default:
				}
				continue
			}

			fi = newFi
			fileName = fi.Name()
			filePath = path.Join(newPath, fileName)
			fileMode = fi.Mode()
		}

		if fileMode.IsDir() {
			pc.wg.Add(1)
			go func(p string) {
				pc.concLimit <- false
				pc.crawlDir(p)
			}(filePath)
			continue
		}

		if !fileMode.IsRegular() {
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

func readDir(path string) ([]os.FileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	list, err := f.Readdir(-1)
	f.Close()
	return list, err
}

func (pc *PosixCrawler) resolveSymlink(currPath string, linkName string) (os.FileInfo, string, error) {
	filePath := currPath
	linkName = path.Join(filePath, linkName)
	fileName, err := os.Readlink(linkName)
	if err != nil {
		return nil, "", err
	}
	if !path.IsAbs(fileName) {
		fileName = path.Join(filePath, fileName)
		fileName = filepath.Clean(fileName)
		filePath = filepath.Dir(fileName)
	}

	isSymlink := true
	filesSeen := make(map[string]bool)

	for {
		fi, err := os.Lstat(fileName)
		if err != nil {
			return nil, "", err
		}

		if _, found := filesSeen[fileName]; found {
			return nil, "", fmt.Errorf("circular symlink: %v", linkName)
		}
		filesSeen[fileName] = false

		isSymlink = fi.Mode()&os.ModeSymlink == os.ModeSymlink
		if isSymlink {
			fileName, err = os.Readlink(fileName)
			if err != nil {
				return nil, "", err
			}
			if !path.IsAbs(fileName) {
				fileName = path.Join(filePath, fileName)
				fileName = filepath.Clean(fileName)
				filePath = filepath.Dir(fileName)
			}
			continue
		} else {
			return fi, filePath, nil
		}
	}

}

func (pc *PosixCrawler) outputResult() {
	for info := range pc.Outputs {
		out, _ := json.Marshal(info)
		fmt.Printf("%s\n", string(out))
	}
}

func main() {
	if len(os.Args) < 2 {
		panic("please specify root directory to crawl")
	}

	rootDir := os.Args[1]
	var pattern string
	conc := 4

	if len(os.Args) > 2 {
		flagSet := flag.NewFlagSet("Usage", flag.ExitOnError)
		flagSet.StringVar(&pattern, "regexp", "", "Crawl regexp match")
		flagSet.IntVar(&conc, "conc", 4, "Concurrency of crawler")

		flagSet.Parse(os.Args[2:])
	}
	crawler := NewPosixCrawler(conc, pattern, true)
	err := crawler.Crawl(rootDir)
	if err != nil {
		os.Stderr.Write([]byte(err.Error() + "\n"))
	}
}

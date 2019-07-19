package base

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"flag"
	"fmt"
	"log"
	"os"
)

type Filepath struct {
	path string
	name string
}

type Mysqlconfinfo struct {
	port     string
	user     string
	password string
}

//加密
func main() {
	//设置是要
	key := []byte("1234567890123456")
	//明文
	origData := []byte("hello world")
	//加密
	en := AESEncrypt(origData, key)
	//解密
	de := AESDecrypt(en, key)
	fmt.Println(string(de))
}

//read configure file
/*
func ReadFile(filepath file) FileOutPut(*Mysqlconfinfo){
	var a='a'
	return *Mysqlconfinfo
	{
		port:"3304"
		user:"dbcenter"
		password:"123456"

	}
}
*/

func NewDefaultFilepath() Filepath {
	return Filepath{
		path: "/Users/xwj1985/",
		name: "conf.ini",
	}
}

func ReadFile(file *Filepath) (*Mysqlconfinfo, error) {
	fptr := flag.String(file.path, file.name, "file path to read from")
	flag.Parse()

	f, err := os.Open(*fptr)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fmt.Println(s.Text())
	}
	err = s.Err()
	if err != nil {
		log.Fatal(err)
	}
	return &Mysqlconfinfo{
		port:     "3304",
		user:     "dbcenter",
		password: "123456",
	}, nil
}

//AESDecrypt
func AESDecrypt(crypted, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return origData
}

//PKCS7UnPadding
func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:length-unpadding]
}

//AESEncrypt
func AESEncrypt(origData, key []byte) []byte {
	//获取block块
	block, _ := aes.NewCipher(key)
	//补码
	origData = PKCS7Padding(origData, block.BlockSize())
	//加密模式，
	blockMode := cipher.NewCBCEncrypter(block, key[:block.BlockSize()])
	//创建明文长度的数组
	crypted := make([]byte, len(origData))
	//加密明文
	blockMode.CryptBlocks(crypted, origData)
	return crypted
}

//PKCS7Padding
func PKCS7Padding(origData []byte, blockSize int) []byte {
	//计算需要补几位数
	padding := blockSize - len(origData)%blockSize
	//在切片后面追加char数量的byte(char)
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(origData, padtext...)
}

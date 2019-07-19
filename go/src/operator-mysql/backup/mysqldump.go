package mysqlbackup

import (
	"fmt"
	//base "operator-mysql/base"
	"os"
)

type dbtype struct {
	typevalue bool     //0 all 1 some of databatbase
	valuelist []string // some of database
}

type backuptype struct {
	tooltype bool //0 mysqldump  1 xtracbackup
	dbtype
	backuptype bool //0 full 1 diff
}

func main() {

	// `os.Args` provides access to raw command-line
	// arguments. Note that the first value in this slice
	// is the path to the program, and `os.Args[1:]`
	// holds the arguments to the program.
	argsWithProg := os.Args
	argsWithoutProg := os.Args[1:]

	// You can get individual args with normal indexing.
	arg := os.Args[3]

	fmt.Println(argsWithProg)
	fmt.Println(argsWithoutProg)
	fmt.Println(arg)
	//base.NewDefaultFilepath()

}

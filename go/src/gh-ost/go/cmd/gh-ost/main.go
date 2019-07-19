/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/github/gh-ost/go/base"
	"github.com/github/gh-ost/go/logic"
	"github.com/outbrain/golib/log"

	"golang.org/x/crypto/ssh/terminal"
)

var AppVersion string

// acceptSignals registers for OS signals
func acceptSignals(migrationContext *base.MigrationContext) {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for sig := range c {
			switch sig {
			case syscall.SIGHUP:
				log.Infof("Received SIGHUP. Reloading configuration")
				if err := migrationContext.ReadConfigFile(); err != nil {
					log.Errore(err)
				} else {
					migrationContext.MarkPointOfInterest()
				}
			}
		}
	}()
}

// main is the application's entry point. It will either spawn a CLI or HTTP interfaces.
func main() {
	migrationContext := base.NewMigrationContext()
	flag.StringVar(&migrationContext.InspectorConnectionConfig.Key.Hostname, "host", "10.19.166.66", "主机名推荐用master")
	flag.StringVar(&migrationContext.AssumeMasterHostname, "assume-master-host", "", "明确指定master-host")
	flag.IntVar(&migrationContext.InspectorConnectionConfig.Key.Port, "port", 8001, "MySQL port")
	flag.StringVar(&migrationContext.CliUser, "user", "us_monitor", "MySQL user")
	flag.StringVar(&migrationContext.CliPassword, "password", "Eleme@Monitor776w98HAme", "MySQL password")
	flag.StringVar(&migrationContext.CliMasterUser, "master-user", "", "")
	flag.StringVar(&migrationContext.CliMasterPassword, "master-password", "", "MySQL password")
	flag.StringVar(&migrationContext.ConfigFile, "conf", "", "Config file")
	flag.StringVar(&migrationContext.DatabaseName, "database", "dbmonitor", "database name (mandatory)")
	flag.StringVar(&migrationContext.OriginalTableName, "table", "slave_delay_time", "table name (mandatory)")
	flag.StringVar(&migrationContext.AlterStatement, "alter", "engine=innodb", "")
	flag.BoolVar(&migrationContext.CountTableRows, "exact-rowcount", false, "表的精确行数(避免算法获取带来的误差)")
	flag.BoolVar(&migrationContext.ConcurrentCountTableRows, "concurrent-rowcount", true, "通过后台线程精确统计表行数 执行count(*)操作")
	flag.BoolVar(&migrationContext.AllowedRunningOnMaster, "allow-on-master", true, "准许在master上进行解析binlog")
	flag.BoolVar(&migrationContext.AllowedMasterMaster, "allow-master-master", false, "准许在双master结构上执行")
	flag.BoolVar(&migrationContext.NullableUniqueKeyAllowed, "allow-nullable-unique-key", true, "主键是唯一索引时是否准许null值(导致数据丢失)")
	flag.BoolVar(&migrationContext.ApproveRenamedColumns, "approve-renamed-columns", false, "是否准许rename column操作")
	flag.BoolVar(&migrationContext.SkipRenamedColumns, "skip-renamed-columns", false, "禁止rename column操作")
	flag.BoolVar(&migrationContext.IsTungsten, "tungsten", false, "")
	flag.BoolVar(&migrationContext.DiscardForeignKeys, "discard-foreign-keys", false, "禁止对有外键的表操作")
	flag.BoolVar(&migrationContext.SkipForeignKeyChecks, "skip-foreign-key-checks", false, "明确表没外键时直接禁止该参数减少校验环节")
	flag.BoolVar(&migrationContext.AliyunRDS, "aliyun-rds", false, "只针对阿里云环境")

	flag.BoolVar(&migrationContext.TestOnReplica, "test-on-replica", false, "")
	flag.BoolVar(&migrationContext.TestOnReplicaSkipReplicaStop, "test-on-replica-skip-replica-stop", false, "")
	flag.BoolVar(&migrationContext.MigrateOnReplica, "migrate-on-replica", false, "")
	flag.BoolVar(&migrationContext.OkToDropTable, "ok-to-drop-table", false, "删除历史表")
	flag.BoolVar(&migrationContext.InitiallyDropOldTable, "initially-drop-old-table", true, "启动初始化阶段删除历史表")
	flag.BoolVar(&migrationContext.InitiallyDropGhostTable, "initially-drop-ghost-table", true, "")
	flag.BoolVar(&migrationContext.TimestampOldTable, "timestamp-old-table", false, "中间表采用时间后缀命名")
	flag.BoolVar(&migrationContext.ForceNamedCutOverCommand, "force-named-cut-over", false, "")
	flag.BoolVar(&migrationContext.SwitchToRowBinlogFormat, "switch-to-rbr", false, "自动切换binlog到row格式!完成后不会切回来！！")
	flag.BoolVar(&migrationContext.AssumeRBR, "assume-rbr", false, "")
	flag.BoolVar(&migrationContext.CutOverExponentialBackoff, "cut-over-exponential-backoff", false, "cutover操作时尝试的时间主次递推2^(n-1)秒")
	flag.StringVar(&migrationContext.ThrottleFlagFile, "throttle-flag-file", "", "通过文件存在来进行限流")
	flag.StringVar(&migrationContext.ThrottleAdditionalFlagFile, "throttle-additional-flag-file", "/Users/caipeng/Desktop/throttle.log", "")
	flag.StringVar(&migrationContext.PostponeCutOverFlagFile, "postpone-cut-over-flag-file", "", "推迟cut-over操作..!")
	flag.StringVar(&migrationContext.PanicFlagFile, "panic-flag-file", "", "")
	flag.BoolVar(&migrationContext.DropServeSocket, "initially-drop-socket-file", true, "")
	flag.StringVar(&migrationContext.ServeSocketFile, "serve-socket-file", "/Users/caipeng/Desktop/socket.socket", "")
	flag.Int64Var(&migrationContext.ServeTCPPort, "serve-tcp-port", 8090, "")
	flag.StringVar(&migrationContext.HooksPath, "hooks-path", "", "")
	flag.StringVar(&migrationContext.HooksHintMessage, "hooks-hint", "", "")
	flag.UintVar(&migrationContext.ReplicaServerId, "replica-server-id", 8, "")
	flag.Int64Var(&migrationContext.CriticalLoadIntervalMilliseconds, "critical-load-interval-millis", 0, "")
	flag.Int64Var(&migrationContext.CriticalLoadHibernateSeconds, "critical-load-hibernate-seconds", 0, "")

	maxLoad := flag.String("max-load", "", "同pt的参数类似'Threads_running=100,Threads_connected=500'")
	exponentialBackoffMaxInterval := flag.Int64("exponential-backoff-max-interval", 64, "")
	chunkSize := flag.Int64("chunk-size", 1000, "")
	dmlBatchSize := flag.Int64("dml-batch-size", 300, "")
	defaultRetries := flag.Int64("default-retries", 10, "")
	cutOverLockTimeoutSeconds := flag.Int64("cut-over-lock-timeout-seconds", 3, " cut-over锁超时时间")
	niceRatio := flag.Float64("nice-ratio", 0, "")
	maxLagMillis := flag.Int64("max-lag-millis", 1500, "")
	replicationLagQuery := flag.String("replication-lag-query", "", "触发限流操作sql")
	throttleControlReplicas := flag.String("throttle-control-replicas", "", "在哪个实例上进行限流检测操作")
	throttleQuery := flag.String("throttle-query", "", "提供一个轻量级的sql每秒查询结果>0开始限流")
	throttleHTTP := flag.String("throttle-http", "", "通过http接口返回200来限流")
	heartbeatIntervalMillis := flag.Int64("heartbeat-interval-millis", 100, "心跳检测间隔毫秒")
	criticalLoad := flag.String("critical-load", "", "同pt参数")
	quiet := flag.Bool("quiet", false, "quiet")
	verbose := flag.Bool("verbose", false, "verbose")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	stack := flag.Bool("stack", false, "add stack trace upon error")
	help := flag.Bool("help", false, "Display usage")
	version := flag.Bool("version", false, "Print version & exit")
	checkFlag := flag.Bool("check-flag", false, "")
	flag.StringVar(&migrationContext.ForceTmpTableName, "force-table-names", "", "临时表加前缀字符")
	flag.CommandLine.SetOutput(os.Stdout)

	flag.Parse()
	if *checkFlag {
		return
	}
	if *help {
		fmt.Fprintf(os.Stdout, "Usage of gh-ost:\n")
		flag.PrintDefaults()
		return
	}
	if *version {
		appVersion := AppVersion
		if appVersion == "" {
			appVersion = "unversioned"
		}
		fmt.Println(appVersion)
		return
	}

	log.SetLevel(log.ERROR)
	if *verbose {
		log.SetLevel(log.INFO)
	}
	if *debug {
		log.SetLevel(log.DEBUG)
	}
	if *stack {
		log.SetPrintStackTrace(*stack)
	}
	if *quiet {
		// Override!!
		log.SetLevel(log.ERROR)
	}

	if migrationContext.DatabaseName == "" {
		log.Fatalf("--database must be provided and database name must not be empty")
	}
	if migrationContext.OriginalTableName == "" {
		log.Fatalf("--table must be provided and table name must not be empty")
	}
	if migrationContext.AlterStatement == "" {
		log.Fatalf("--alter must be provided and statement must not be empty")
	}
	executeFlag := flag.Bool("execute", true, "不做实际row-copy操作只是做简单测试然后退出--多用于验证")
	migrationContext.Noop = !(*executeFlag)
	if migrationContext.AllowedRunningOnMaster && migrationContext.TestOnReplica {
		log.Fatalf("--allow-on-master and --test-on-replica are mutually exclusive")
	}
	if migrationContext.AllowedRunningOnMaster && migrationContext.MigrateOnReplica {
		log.Fatalf("--allow-on-master and --migrate-on-replica are mutually exclusive")
	}
	if migrationContext.MigrateOnReplica && migrationContext.TestOnReplica {
		log.Fatalf("--migrate-on-replica and --test-on-replica are mutually exclusive")
	}
	if migrationContext.SwitchToRowBinlogFormat && migrationContext.AssumeRBR {
		log.Fatalf("--switch-to-rbr and --assume-rbr are mutually exclusive")
	}
	if migrationContext.TestOnReplicaSkipReplicaStop {
		if !migrationContext.TestOnReplica {
			log.Fatalf("--test-on-replica-skip-replica-stop requires --test-on-replica to be enabled")
		}
		log.Warning("--test-on-replica-skip-replica-stop enabled. We will not stop replication before cut-over. Ensure you have a plugin that does this.")
	}
	if migrationContext.CliMasterUser != "" && migrationContext.AssumeMasterHostname == "" {
		log.Fatalf("--master-user requires --assume-master-host")
	}
	if migrationContext.CliMasterPassword != "" && migrationContext.AssumeMasterHostname == "" {
		log.Fatalf("--master-password requires --assume-master-host")
	}
	if *replicationLagQuery != "" {
		log.Warningf("--replication-lag-query is deprecated")
	}

	cutOver := flag.String("cut-over", "atomic", "choose cut-over type (default|atomic, two-step)")
	switch *cutOver {
	case "atomic", "default", "":
		migrationContext.CutOverType = base.CutOverAtomic
	case "two-step":
		migrationContext.CutOverType = base.CutOverTwoStep
	default:
		log.Fatalf("Unknown cut-over: %s", *cutOver)
	}
	//从环境变量读取用户名，密码
	if err := migrationContext.ReadConfigFile(); err != nil {
		log.Fatale(err)
	}
	//限流实例host:port,host:port...
	if err := migrationContext.ReadThrottleControlReplicaKeys(*throttleControlReplicas); err != nil {
		log.Fatale(err)
	}
	//限流参数初始化--max-load=?,threads_running=?,threads_connected=?
	if err := migrationContext.ReadMaxLoad(*maxLoad); err != nil {
		log.Fatale(err)
	}
	if err := migrationContext.ReadCriticalLoad(*criticalLoad); err != nil {
		log.Fatale(err)
	}

	if migrationContext.ServeSocketFile == "" {
		migrationContext.ServeSocketFile = fmt.Sprintf("/tmp/gh-ost.%s.%s.sock", migrationContext.DatabaseName, migrationContext.OriginalTableName)
	}
	askPass := flag.Bool("ask-pass", false, "prompt for MySQL password")

	if *askPass {
		fmt.Println("Password:")
		bytePassword, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			log.Fatale(err)
		}
		migrationContext.CliPassword = string(bytePassword)
	}
	migrationContext.SetHeartbeatIntervalMilliseconds(*heartbeatIntervalMillis)
	migrationContext.SetNiceRatio(*niceRatio)
	migrationContext.SetChunkSize(*chunkSize)
	migrationContext.SetDMLBatchSize(*dmlBatchSize)
	migrationContext.SetMaxLagMillisecondsThrottleThreshold(*maxLagMillis)
	migrationContext.SetThrottleQuery(*throttleQuery)
	migrationContext.SetThrottleHTTP(*throttleHTTP)
	migrationContext.SetDefaultNumRetries(*defaultRetries)
	migrationContext.ApplyCredentials()
	//cut-over最大获取锁超时时间控制在1-10秒之间
	if err := migrationContext.SetCutOverLockTimeoutSeconds(*cutOverLockTimeoutSeconds); err != nil {
		log.Errore(err)
	}
	//cut-over过程每次重试rename间的时间递增间隔(必须>2)
	if err := migrationContext.SetExponentialBackoffMaxInterval(*exponentialBackoffMaxInterval); err != nil {
		log.Errore(err)
	}

	log.Infof("starting gh-ost %+v", AppVersion)
	acceptSignals(migrationContext)

	migrator := logic.NewMigrator(migrationContext)
	err := migrator.Migrate()
	if err != nil {
		migrator.ExecOnFailureHook()
		log.Fatale(err)
	}
	fmt.Fprintf(os.Stdout, "# Done\n")
}

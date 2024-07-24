package dcron

import (
	"fmt"
	"github.com/libi/dcron/consistenthash"
	"github.com/libi/dcron/driver"
	"sync"
	"time"
)

// NodePool is a node pool
type NodePool struct {
	serviceName string
	NodeID      string

	mu    sync.Mutex
	nodes *consistenthash.Map

	Driver         driver.Driver
	hashReplicas   int
	hashFn         consistenthash.Hash
	updateDuration time.Duration

	dcron *Dcron
}

func newNodePool(serverName string, driver driver.Driver, dcron *Dcron, updateDuration time.Duration, hashReplicas int) *NodePool {

	err := driver.Ping()
	if err != nil {
		panic(err)
	}

	nodePool := &NodePool{
		Driver:         driver,
		serviceName:    serverName,
		dcron:          dcron,
		hashReplicas:   hashReplicas,
		updateDuration: updateDuration,
	}
	return nodePool
}

func (np *NodePool) StartPool() error {
	var err error
	np.Driver.SetTimeout(np.updateDuration)
	np.NodeID, err = np.Driver.RegisterServiceNode(np.serviceName)
	if err != nil {
		return err
	}
	np.Driver.SetHeartBeat(np.NodeID)

	err = np.updatePool()
	if err != nil {
		return err
	}

	go np.tickerUpdatePool()
	return nil
}

func (np *NodePool) updatePool() error {
	np.mu.Lock()
	defer np.mu.Unlock()
	nodes, err := np.Driver.GetServiceNodeList(np.serviceName)
	if err != nil {
		return err
	}
	np.nodes = consistenthash.New(np.hashReplicas, np.hashFn)
	for _, node := range nodes {
		np.nodes.Add(node)
	}
	return nil
}
func (np *NodePool) tickerUpdatePool() {
	tickers := time.NewTicker(np.updateDuration)
	for range tickers.C {
		if np.dcron.isRun {
			err := np.updatePool()
			if err != nil {
				np.dcron.err("update node pool error %+v", err)
			}
		} else {
			tickers.Stop()
			return
		}
	}
}

// PickNodeByJobName : 使用一致性hash算法根据任务名获取一个执行节点
func (np *NodePool) PickNodeByJobName(jobName string) string {
	timeNow := time.Now()
	tag := fmt.Sprintf("%d-%s", timeNow.Unix(), jobName)
	fmt.Printf("PickNodeByJobName Start %s cost:%s ", tag, time.Now().Sub(timeNow).String())
	np.mu.Lock()
	fmt.Printf("PickNodeByJobName Get Lock %s cost:%s ", tag, time.Now().Sub(timeNow).String())
	defer np.mu.Unlock()
	if np.nodes.IsEmpty() {
		return ""
	}

	fmt.Printf("PickNodeByJobName Get Nodes %s cost:%s ", tag, time.Now().Sub(timeNow).String())
	str := np.nodes.Get(jobName)
	fmt.Printf("PickNodeByJobName End Get Nodes %s cost:%s ", tag, time.Now().Sub(timeNow).String())
	return str
}

package dcron

import (
	"fmt"
	"github.com/robfig/cron/v3"
)

type Job interface {
	Run()
}

// JobWarpper is a job warpper
type JobWarpper struct {
	ID      cron.EntryID
	Dcron   *Dcron
	Name    string
	CronStr string
	Func    func()
	Job     Job
}

// Run is run job
func (job JobWarpper) Run() {
	//如果该任务分配给了这个节点 则允许执行
	fmt.Printf("JobWarpper 执行 job %s", job.Name)
	if job.Dcron.allowThisNodeRun(job.Name) {
		if job.Func != nil {
			job.Func()
		}
		if job.Job != nil {
			job.Job.Run()
		}
	} else {
		fmt.Printf("JobWarpper 未执行 job %s", job.Name)
	}
}

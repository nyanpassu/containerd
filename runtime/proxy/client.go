package proxy

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"

	// tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	typesapi "github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/runtime"
	"github.com/containerd/containerd/runtime/v2/task"
)

func NewPlatformRuntime(task task.TaskService, name string) runtime.PlatformRuntime {
	return &proxyPlatformRuntime{
		id:   name,
		task: task,
	}
}

type proxyPlatformRuntime struct {
	id        string
	task      task.TaskService
	namespace string
}

// ID of the runtime
func (p *proxyPlatformRuntime) ID() string {
	return p.id
}

// Create creates a task with the provided id and options.
func (p *proxyPlatformRuntime) Create(ctx context.Context, id string, opts runtime.CreateOpts) (runtime.Task, error) {
	var rootfs []*typesapi.Mount
	if opts.Rootfs != nil {
		for _, mnt := range opts.Rootfs {
			rootfs = append(rootfs, &typesapi.Mount{
				Type:   mnt.Type,
				Source: mnt.Source,
				// TODO how to get target ?
			})
		}
	}
	resp, err := p.task.Create(ctx, &task.CreateTaskRequest{
		ID:       id,
		Rootfs:   rootfs,
		Stdin:    opts.IO.Stdin,
		Stdout:   opts.IO.Stdout,
		Stderr:   opts.IO.Stderr,
		Terminal: opts.IO.Terminal,
	})
	if err != nil {
		return nil, err
	}
	return newProxyTask(p.task, id, resp.Pid, p.namespace), nil
}

// Get returns a task.
func (p *proxyPlatformRuntime) Get(ctx context.Context, id string) (runtime.Task, error) {
	newProxyTask(p.task, id)

	resp, err := p.client.Get(ctx, &tasksapi.GetRequest{
		ContainerID: id,
	})
	if err != nil {
		return nil, err
	}
	// server will not use Process.ContainerID but process.ID
	return newProxyTask(p.client, resp.Process.ID, resp.Process.Pid, p.namespace), nil
}

// Tasks returns all the current tasks for the runtime.
// Any container runs at most one task at a time.
func (p *proxyPlatformRuntime) Tasks(ctx context.Context, b bool) ([]runtime.Task, error) {

	resp, err := p.client.List(ctx, &tasksapi.ListTasksRequest{})
	if err != nil {
		return nil, err
	}
	var tasks []runtime.Task
	for _, process := range resp.Tasks {
		tasks = append(tasks, newProxyTask(p.client, process.ID, process.Pid, p.namespace))
	}
	return tasks, nil
}

// Add adds a task into runtime.
func (p *proxyPlatformRuntime) Add(ctx context.Context, task runtime.Task) error {
	return errors.New("unsupported")
}

// Delete remove a task.
func (p *proxyPlatformRuntime) Delete(ctx context.Context, id string) {
	// do nothing
}

func newProxyTask(ts task.TaskService, id string, pid uint32, namespace string) runtime.Task {
	return &proxyTask{
		proxyProcess: proxyProcess{
			task: ts,
			tid:  id,
		},
		pid:       pid,
		namespace: namespace,
	}
}

type proxyTask struct {
	proxyProcess
	pid       uint32
	namespace string
}

// ID of the process
func (t *proxyTask) ID() string {
	return t.tid
}

// PID of the process
func (t *proxyTask) PID() uint32 {
	return t.pid
}

// Namespace that the task exists in
func (t *proxyTask) Namespace() string {
	return t.namespace
}

// Start the container's user defined process
func (p *proxyTask) Start(ctx context.Context) error {
	resp, err := p.task.Start(ctx, &task.StartRequest{
		ID: p.tid,
	})
	if err == nil {
		p.pid = resp.Pid
	}
	return err
}

// Pause pauses the container process
func (t *proxyTask) Pause(ctx context.Context) error {
	_, err := t.task.Pause(ctx, &task.PauseRequest{
		ID: t.tid,
	})
	return err
}

// Resume unpauses the container process
func (t *proxyTask) Resume(ctx context.Context) error {
	_, err := t.task.Resume(ctx, &task.ResumeRequest{
		ID: t.tid,
	})
	return err
}

// Exec adds a process into the container
func (t *proxyTask) Exec(ctx context.Context, execID string, opts runtime.ExecOpts) (runtime.Process, error) {
	_, err := t.task.Exec(ctx, &task.ExecProcessRequest{
		ID:       t.tid,
		ExecID:   execID,
		Stdin:    opts.IO.Stdin,
		Stdout:   opts.IO.Stdout,
		Stderr:   opts.IO.Stderr,
		Terminal: opts.IO.Terminal,
		Spec:     opts.Spec,
	})
	if err != nil {
		return nil, err
	}
	return newProxyProcess(
		t.task,
		t.tid,
		execID,
	), nil
}

// Pids returns all pids
func (t *proxyTask) Pids(ctx context.Context) ([]runtime.ProcessInfo, error) {
	resp, err := t.task.Pids(ctx, &task.PidsRequest{
		ID: t.tid,
	})
	if err != nil {
		return nil, err
	}
	var r []runtime.ProcessInfo
	for _, p := range resp.Processes {
		r = append(r, runtime.ProcessInfo{
			Pid:  p.Pid,
			Info: p.Info,
		})
	}
	return r, nil
}

// Checkpoint checkpoints a container to an image with live system data
func (t *proxyTask) Checkpoint(ctx context.Context, path string, any *types.Any) error {
	_, err := t.task.Checkpoint(ctx, &task.CheckpointTaskRequest{
		ID:      t.tid,
		Path:    path,
		Options: any,
	})
	return err
}

// Update sets the provided resources to a running task
func (t *proxyTask) Update(ctx context.Context, any *types.Any) error {
	_, err := t.task.Update(ctx, &task.UpdateTaskRequest{
		ID:        t.tid,
		Resources: any,
	})
	return err
}

// Process returns a process within the task for the provided id
func (t *proxyTask) Process(ctx context.Context, execid string) (runtime.Process, error) {
	p := newProxyProcess(t.task, t.tid, execid)
	_, err := p.State(ctx)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Stats returns runtime specific metrics for a task
func (t *proxyTask) Stats(ctx context.Context) (*types.Any, error) {
	resp, err := t.task.Stats(ctx, &task.StatsRequest{
		ID: t.tid,
	})
	if err != nil {
		return nil, err
	}
	return resp.Stats, nil
}

func newProxyProcess(ts task.TaskService, tid string, execid string) runtime.Process {
	return &proxyProcess{
		task:   ts,
		tid:    tid,
		execid: execid,
	}
}

type proxyProcess struct {
	tid    string
	execid string
	task   task.TaskService
}

// ID of the process
func (p *proxyProcess) ID() string {
	return p.execid
}

// State returns the process state
func (p *proxyProcess) State(ctx context.Context) (runtime.State, error) {
	resp, err := p.task.State(ctx, &task.StateRequest{
		ID:     p.tid,
		ExecID: p.execid,
	})
	if err != nil {
		return runtime.State{}, nil
	}
	return runtime.State{
		Status:     runtime.Status(resp.Status),
		Pid:        resp.Pid,
		ExitStatus: resp.ExitStatus,
		ExitedAt:   resp.ExitedAt,
		Stdin:      resp.Stdin,
		Stdout:     resp.Stdout,
		Stderr:     resp.Stderr,
		Terminal:   resp.Terminal,
	}, nil
}

// Kill signals a container
func (p *proxyProcess) Kill(ctx context.Context, signal uint32, all bool) error {
	_, err := p.task.Kill(ctx, &task.KillRequest{
		ID:     p.tid,
		ExecID: p.tid,
		Signal: signal,
		All:    all,
	})
	return err
}

// Pty resizes the processes pty/console
func (p *proxyProcess) ResizePty(ctx context.Context, size runtime.ConsoleSize) error {
	_, err := p.task.ResizePty(ctx, &task.ResizePtyRequest{
		ID:     p.tid,
		ExecID: p.execid,
		Width:  size.Width,
		Height: size.Height,
	})
	return err
}

// CloseStdin closes the processes stdin
func (p *proxyProcess) CloseIO(ctx context.Context) error {
	_, err := p.task.CloseIO(ctx, &task.CloseIORequest{
		ID:     p.tid,
		ExecID: p.execid,
	})
	return err
}

// Start the container's user defined process
func (p *proxyProcess) Start(ctx context.Context) error {
	_, err := p.task.Start(ctx, &task.StartRequest{
		ID:     p.tid,
		ExecID: p.execid,
	})
	return err
}

// Wait for the process to exit
func (p *proxyProcess) Wait(ctx context.Context) (*runtime.Exit, error) {
	resp, err := p.task.Wait(ctx, &task.WaitRequest{
		ID:     p.tid,
		ExecID: p.tid,
	})
	if err != nil {
		return nil, err
	}
	// Process don't return pid
	return &runtime.Exit{
		Status:    resp.ExitStatus,
		Timestamp: resp.ExitedAt,
	}, nil
}

// Delete deletes the process
func (p *proxyProcess) Delete(ctx context.Context) (*runtime.Exit, error) {
	resp, err := p.task.Delete(ctx, &task.DeleteRequest{
		ID:     p.tid,
		ExecID: p.execid,
	})
	if err != nil {
		return nil, err
	}
	return &runtime.Exit{
		Pid:       resp.Pid,
		Status:    resp.ExitStatus,
		Timestamp: resp.ExitedAt,
	}, nil
}

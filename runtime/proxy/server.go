package proxy

import (
	"context"
	"net"

	pbtypes "github.com/gogo/protobuf/types"
	"google.golang.org/grpc"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	apitypes "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/containerd/runtime"
)

var empty = &pbtypes.Empty{}

func RunPlatformRuntimeService(runtime runtime.PlatformRuntime) error {
	listener, err := net.Listen("unix", "")
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	tasksapi.RegisterTasksServer(server, &taskServer{})
	return server.Serve(listener)
}

type taskServer struct {
	runtime runtime.PlatformRuntime
}

// Create a task.
func (ts *taskServer) Create(ctx context.Context, r *tasksapi.CreateTaskRequest) (*tasksapi.CreateTaskResponse, error) {
	task, err := ts.runtime.Create(ctx, r.ContainerID, runtime.CreateOpts{
		Spec: nil,
		// Rootfs mounts to perform to gain access to the container's filesystem
		Rootfs: nil,
		// IO for the container's main process
		IO: runtime.IO{
			Stdin:    r.Stdin,
			Stdout:   r.Stdout,
			Stderr:   r.Stderr,
			Terminal: r.Terminal,
		},
		// Checkpoint digest to restore container state
		Checkpoint: "",
		// RuntimeOptions for the runtime
		RuntimeOptions: nil,
		// TaskOptions received for the task
		TaskOptions: nil,
	})
	if err != nil {
		return nil, err
	}
	return &tasksapi.CreateTaskResponse{
		ContainerID: task.ID(),
		Pid:         task.PID(),
	}, nil
}

// Start a process.
func (ts *taskServer) Start(ctx context.Context, r *tasksapi.StartRequest) (*tasksapi.StartResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if err := task.Start(ctx); err != nil {
		return nil, err
	}
	return &tasksapi.StartResponse{}, nil
}

// Delete a task and on disk state.
func (ts *taskServer) Delete(ctx context.Context, r *tasksapi.DeleteTaskRequest) (*tasksapi.DeleteResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	exit, err := task.Delete(ctx)
	if err != nil {
		return nil, err
	}
	return &tasksapi.DeleteResponse{
		Pid:        exit.Pid,
		ExitStatus: exit.Status,
		ExitedAt:   exit.Timestamp,
	}, nil
}

func (ts *taskServer) DeleteProcess(ctx context.Context, r *tasksapi.DeleteProcessRequest) (*tasksapi.DeleteResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	proc, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	exit, err := proc.Delete(ctx)
	if err != nil {
		return nil, err
	}
	return &tasksapi.DeleteResponse{
		ID:         r.ExecID,
		Pid:        exit.Pid,
		ExitStatus: exit.Status,
		ExitedAt:   exit.Timestamp,
	}, nil
}

func (ts *taskServer) Get(ctx context.Context, r *tasksapi.GetRequest) (*tasksapi.GetResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	get := func(f func() (string, runtime.State, error)) (*tasksapi.GetResponse, error) {
		id, state, err := f()
		if err != nil {
			return nil, err
		}
		return &tasksapi.GetResponse{
			Process: &apitypes.Process{
				ContainerID: task.ID(),
				ID:          id,
				Pid:         state.Pid,
				Status:      apitypes.Status(state.Status),
				Stdin:       state.Stdin,
				Stdout:      state.Stdout,
				Stderr:      state.Stderr,
				Terminal:    state.Terminal,
				ExitStatus:  state.ExitStatus,
				ExitedAt:    state.ExitedAt,
			},
		}, nil
	}
	if r.ExecID == "" {
		return get(func() (string, runtime.State, error) {
			id := task.ID()
			state, err := task.State(ctx)
			return id, state, err
		})
	}
	proc, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	return get(func() (string, runtime.State, error) {
		id := proc.ID()
		state, err := proc.State(ctx)
		return id, state, err
	})
}

func (ts *taskServer) List(ctx context.Context, r *tasksapi.ListTasksRequest) (*tasksapi.ListTasksResponse, error) {
	tasks, err := ts.runtime.Tasks(ctx, false)
	if err != nil {
		return nil, err
	}
	var processes []*apitypes.Process
	for _, task := range tasks {
		status, err := task.State(ctx)
		if err != nil {
			return nil, err
		}
		processes = append(processes, &apitypes.Process{
			ID:         task.ID(),
			Pid:        status.Pid,
			Status:     apitypes.Status(status.Status),
			Stdin:      status.Stdin,
			Stdout:     status.Stdout,
			Stderr:     status.Stderr,
			Terminal:   status.Terminal,
			ExitStatus: status.ExitStatus,
			ExitedAt:   status.ExitedAt,
		})
	}
	return &tasksapi.ListTasksResponse{
		Tasks: processes,
	}, nil
}

// Kill a task or process.
func (ts *taskServer) Kill(ctx context.Context, r *tasksapi.KillRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if r.All {
		if err = task.Kill(ctx, r.Signal, true); err != nil {
			return nil, err
		}
		return empty, nil
	}
	proc, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	if err := proc.Kill(ctx, r.Signal, false); err != nil {
		return nil, err
	}
	return empty, nil
}

func (ts *taskServer) Exec(ctx context.Context, r *tasksapi.ExecProcessRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if _, err = task.Exec(ctx, r.ExecID, runtime.ExecOpts{
		Spec: r.Spec,
		IO: runtime.IO{
			Stdin:    r.Stdin,
			Stdout:   r.Stdout,
			Stderr:   r.Stderr,
			Terminal: r.Terminal,
		},
	}); err != nil {
		return nil, err
	}
	return empty, nil
}

func (ts *taskServer) ResizePty(ctx context.Context, r *tasksapi.ResizePtyRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	resize := func(f func(runtime.ConsoleSize) error) (*pbtypes.Empty, error) {
		if err := f(runtime.ConsoleSize{
			Width:  r.Width,
			Height: r.Height,
		}); err != nil {
			return nil, err
		}
		return empty, nil
	}
	if r.ExecID == "" {
		return resize(func(size runtime.ConsoleSize) error { return task.ResizePty(ctx, size) })
	}
	process, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	return resize(func(size runtime.ConsoleSize) error { return process.ResizePty(ctx, size) })
}

func (ts *taskServer) CloseIO(ctx context.Context, r *tasksapi.CloseIORequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	close := func(f func() error) (*pbtypes.Empty, error) {
		if err := f(); err != nil {
			return nil, err
		}
		return empty, nil
	}
	if r.ExecID != "" {
		return close(func() error { return task.CloseIO(ctx) })
	}
	proc, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	return close(func() error { return proc.CloseIO(ctx) })
}

func (ts *taskServer) Pause(ctx context.Context, r *tasksapi.PauseTaskRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if err := task.Pause(ctx); err != nil {
		return nil, err
	}
	return empty, nil
}

func (ts *taskServer) Resume(ctx context.Context, r *tasksapi.ResumeTaskRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if err := task.Resume(ctx); err != nil {
		return nil, err
	}
	return empty, nil
}

func (ts *taskServer) ListPids(ctx context.Context, r *tasksapi.ListPidsRequest) (*tasksapi.ListPidsResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	processes, err := task.Pids(ctx)
	if err != nil {
		return nil, err
	}
	var infos []*apitypes.ProcessInfo
	for _, proc := range processes {
		infos = append(infos, &apitypes.ProcessInfo{
			Pid: proc.Pid,
			// TODO
			// Info: proc.Info,
		})
	}
	return &tasksapi.ListPidsResponse{
		Processes: infos,
	}, nil
}

func (ts *taskServer) Checkpoint(ctx context.Context, r *tasksapi.CheckpointTaskRequest) (*tasksapi.CheckpointTaskResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	task.Checkpoint(ctx)
	// task.Checkpoint(ctx, )
}

func (ts *taskServer) Update(ctx context.Context, r *tasksapi.UpdateTaskRequest) (*pbtypes.Empty, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	if err = task.Update(ctx, r.Resources); err != nil {
		return nil, err
	}
	return empty, nil
}

func (ts *taskServer) Metrics(ctx context.Context, r *tasksapi.MetricsRequest) (*tasksapi.MetricsResponse, error) {

}

func (ts *taskServer) Wait(ctx context.Context, r *tasksapi.WaitRequest) (*tasksapi.WaitResponse, error) {
	task, err := ts.runtime.Get(ctx, r.ContainerID)
	if err != nil {
		return nil, err
	}
	wait := func(exit *runtime.Exit, err error) (*tasksapi.WaitResponse, error) {
		if err != nil {
			return nil, err
		}
		return &tasksapi.WaitResponse{
			ExitStatus: exit.Status,
			ExitedAt:   exit.Timestamp,
		}, nil
	}
	if r.ExecID == "" {
		return wait(task.Wait(ctx))
	}
	proc, err := task.Process(ctx, r.ExecID)
	if err != nil {
		return nil, err
	}
	return wait(proc.Wait(ctx))
}

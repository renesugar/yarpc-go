// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package yarpc

import (
	"sync"

	"go.uber.org/multierr"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/internal/errorsync"
	"go.uber.org/zap"
)

type starter struct {
	startedMu sync.Mutex
	started   []transport.Lifecycle

	dispatcher *Dispatcher
	log        *zap.Logger
}

func (s *starter) start(lc transport.Lifecycle) func() error {
	return func() error {
		if lc == nil {
			return nil
		}

		if err := lc.Start(); err != nil {
			return err
		}

		s.startedMu.Lock()
		s.started = append(s.started, lc)
		s.startedMu.Unlock()

		return nil
	}
}

func (s *starter) abort(errs []error) error {
	// Failed to start so stop everything that was started.
	wait := errorsync.ErrorWaiter{}
	s.startedMu.Lock()
	for _, lc := range s.started {
		wait.Submit(lc.Stop)
	}
	s.startedMu.Unlock()
	if newErrors := wait.Wait(); len(newErrors) > 0 {
		errs = append(errs, newErrors...)
	}

	return multierr.Combine(errs...)
}

func (s *starter) setRouters() {
	// don't need a sync.Once, since we call this in a lifecycle.Once in the
	// dispatcher.
	s.log.Debug("setting router for inbounds")
	for _, ib := range s.dispatcher.inbounds {
		ib.SetRouter(s.dispatcher.table)
	}
	s.log.Debug("set router for inbounds")
}

func (s *starter) startTransports() error {
	s.log.Info("starting transports")
	wait := errorsync.ErrorWaiter{}
	for _, t := range s.dispatcher.transports {
		wait.Submit(s.start(t))
	}
	if errs := wait.Wait(); len(errs) != 0 {
		return s.abort(errs)
	}
	s.log.Debug("started transports")
	return nil
}

func (s *starter) startOutbounds() error {
	s.log.Info("starting outbounds")
	wait := errorsync.ErrorWaiter{}
	for _, o := range s.dispatcher.outbounds {
		wait.Submit(s.start(o.Unary))
		wait.Submit(s.start(o.Oneway))
		wait.Submit(s.start(o.Stream))
	}
	if errs := wait.Wait(); len(errs) != 0 {
		return s.abort(errs)
	}
	s.log.Debug("started outbounds")
	return nil

}

func (s *starter) startInbounds() error {
	s.log.Info("starting inbounds")
	wait := errorsync.ErrorWaiter{}
	for _, i := range s.dispatcher.inbounds {
		wait.Submit(s.start(i))
	}
	if errs := wait.Wait(); len(errs) != 0 {
		return s.abort(errs)
	}
	s.log.Debug("started inbounds")
	return nil
}

// TransportStarter implements the first of three steps in dispatcher startup:
// it starts the transports, but not the inbounds or outbounds. The transports
// MUST be started before the inbounds and outbounds.
//
// Unlike the all-in-one Start method on the dispatcher, the caller is
// responsible for ensuring that the TransportStarter is called only once. It
// is not safe for concurrent use.
type TransportStarter struct{ *starter }

// Start starts the transports, which is a precondition for successfully
// starting outbounds and inbounds.
func (ts *TransportStarter) Start() (*OutboundStarter, error) {
	if err := ts.startTransports(); err != nil {
		return nil, err
	}
	return &OutboundStarter{ts.starter}, nil
}

// OutboundStarter implements the second of three steps in dispatcher startup:
// it starts the outbounds. Outbounds MUST be started after transports, but
// before inbounds.
//
// Unlike the all-in-one Start method on the dispatcher, the caller is
// responsible for ensuring that the OutboundStarter is called only once. It
// is not safe for concurrent use.
type OutboundStarter struct{ *starter }

// Start starts the outbounds, which allows users of the dispatcher to
// construct clients and make outbound RPCs.
func (os *OutboundStarter) Start() (*InboundStarter, error) {
	if err := os.startOutbounds(); err != nil {
		return nil, err
	}
	return &InboundStarter{os.starter}, nil
}

// InboundStarter implements the last of three steps in dispatcher startup: it
// starts the inbounds. Inbounds MUST be started after both the transports and
// the outbounds.
//
// Unlike the all-in-one Start method on the dispatcher, the caller is
// responsible for ensuring that the InboundStarter is called only once. It
// is not safe for concurrent use.
type InboundStarter struct{ *starter }

// Start starts the inbounds, which allows procedures registered with the
// dispatcher to receive requests.
func (is *InboundStarter) Start() error {
	return is.startInbounds()
}

type stopper struct {
	dispatcher *Dispatcher
	log        *zap.Logger
}

func (s *stopper) stopInbounds() error {
	s.log.Debug("stopping inbounds")
	wait := errorsync.ErrorWaiter{}
	for _, ib := range s.dispatcher.inbounds {
		wait.Submit(ib.Stop)
	}
	if errs := wait.Wait(); len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	s.log.Debug("stopped inbounds")
	return nil
}

func (s *stopper) stopOutbounds() error {
	s.log.Debug("stopping outbounds")
	wait := errorsync.ErrorWaiter{}
	for _, o := range s.dispatcher.outbounds {
		if o.Unary != nil {
			wait.Submit(o.Unary.Stop)
		}
		if o.Oneway != nil {
			wait.Submit(o.Oneway.Stop)
		}
		if o.Stream != nil {
			wait.Submit(o.Stream.Stop)
		}
	}
	if errs := wait.Wait(); len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	s.log.Debug("stopped outbounds")
	return nil
}

func (s *stopper) stopTransports() error {
	s.log.Debug("stopping transports")
	wait := errorsync.ErrorWaiter{}
	for _, t := range s.dispatcher.transports {
		wait.Submit(t.Stop)
	}
	if errs := wait.Wait(); len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	s.log.Debug("stopped transports")

	s.log.Debug("stopping metrics push loop, if any")
	s.dispatcher.stopMeter()
	s.log.Debug("stopped metrics push loop, if any")

	return nil
}

// InboundStopper implements the first of three steps in dispatcher shutdown:
// it shuts down the inbounds, but not the outbounds or transports. Inbounds
// MUST be stopped before outbounds or transports.
//
// Unlike the all-in-one Stop method on the dispatcher, the caller is
// responsible for ensuring that the InboundStopper is called only once. It is
// not safe for concurrent use.
type InboundStopper struct {
	*stopper
}

// Stop stops the inbounds, which stops dispatching incoming RPCs to
// procedures.
func (is *InboundStopper) Stop() (*OutboundStopper, error) {
	if err := is.stopInbounds(); err != nil {
		return nil, err
	}
	return &OutboundStopper{is.stopper}, nil
}

// OutboundStopper implements the second of three steps in dispatcher shutdown:
// it shuts down the outbounds. Outbounds MUST be stopped after inbounds and
// before transports.
//
// Unlike the all-in-one Stop method on the dispatcher, the caller is
// responsible for ensuring that the OutboundStopper is called only once. It is
// not safe for concurrent use.
type OutboundStopper struct {
	*stopper
}

// Stop stops the outbounds, which prevents clients from calling other
// services.
func (os *OutboundStopper) Stop() (*TransportStopper, error) {
	if err := os.stopInbounds(); err != nil {
		return nil, err
	}
	return &TransportStopper{os.stopper}, nil
}

// TransportStopper implements the last of three steps in dispatcher shutdown:
// it shuts down the transports. Transports MUST be stopped after outbounds
// and inbounds.
//
// Unlike the all-in-one Stop method on the dispatcher, the caller is
// responsible for ensuring that the TransportStopper is called only once. It
// is not safe for concurrent use.
type TransportStopper struct {
	*stopper
}

// Stop stops the transports, which completes dispatcher shutdown.
func (ts *TransportStopper) Stop() error {
	return ts.stopTransports()
}

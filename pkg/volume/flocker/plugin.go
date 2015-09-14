/*
Copyright 2015 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flocker

import (
	"strconv"

	flockerClient "github.com/ClusterHQ/flocker-go"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/types"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	defaultHost           = "localhost"
	defaultPort           = 4523
	defaultCACertFile     = "/etc/flocker/cluster.crt"
	defaultClientKeyFile  = "/etc/flocker/apiuser.key"
	defaultClientCertFile = "/etc/flocker/apiuser.crt"
)

func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&flockerPlugin{}}
}

type flockerPlugin struct {
	host volume.VolumeHost
}

type flocker struct {
	datasetName string
	datasetID   string
	pod         *api.Pod
	mounter     mount.Interface
	plugin      *flockerPlugin
}

func (p *flockerPlugin) Init(host volume.VolumeHost) {
	p.host = host
}

func (p flockerPlugin) Name() string {
	return "kubernetes.io/flocker"
}

func (p flockerPlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Flocker != nil) ||
		(spec.Volume != nil && spec.Volume.Flocker != nil)
}

func (p *flockerPlugin) getFlockerVolumeSource(spec *volume.Spec) (*api.FlockerVolumeSource, bool) {
	// AFAIK this will always be r/w, but perhaps for the future it will be needed
	readOnly := false

	if spec.Volume != nil && spec.Volume.Flocker != nil {
		return spec.Volume.Flocker, readOnly
	}
	return spec.PersistentVolume.Spec.Flocker, readOnly
}

func (p *flockerPlugin) NewBuilder(spec *volume.Spec, pod *api.Pod, opts volume.VolumeOptions) (volume.Builder, error) {
	source, readOnly := p.getFlockerVolumeSource(spec)
	builder := flockerBuilder{
		flocker: &flocker{
			datasetName: source.DatasetName,
			datasetID:   "",
			pod:         pod,
			mounter:     p.host.GetMounter(),
			plugin:      p,
		},
		exe:      exec.New(),
		opts:     opts,
		readOnly: readOnly,
	}
	return &builder, nil
}

func (p *flockerPlugin) NewCleaner(datasetName string, podUID types.UID) (volume.Cleaner, error) {
	// Flocker agent will take care of this, there is nothing we can do here
	return nil, nil
}

type flockerBuilder struct {
	*flocker
	exe      exec.Interface
	opts     volume.VolumeOptions
	readOnly bool
}

func (b flockerBuilder) GetPath() string {
	if b.flocker.datasetID == "" {
		return ""
	}
	return "/flocker/" + b.flocker.datasetID
}

func (b flockerBuilder) SetUp() error {
	return b.SetUpAt(b.flocker.datasetName)
}

func (b flockerBuilder) SetUpAt(dir string) error {
	host := getenvOrFallback("FLOCKER_CONTROL_SERVICE_HOST", defaultHost)
	portConfig := getenvOrFallback("FLOCKER_CONTROL_SERVICE_PORT", strconv.Itoa(defaultPort))
	port, err := strconv.Atoi(portConfig)
	if err != nil {
		return err
	}
	caCertPath := getenvOrFallback("FLOCKER_CONTROL_SERVICE_CA_FILE", defaultCACertFile)
	keyPath := getenvOrFallback("FLOCKER_CONTROL_SERVICE_CLIENT_KEY_FILE", defaultClientKeyFile)
	certPath := getenvOrFallback("FLOCKER_CONTROL_SERVICE_CLIENT_CERT_FILE", defaultClientCertFile)

	c, err := flockerClient.NewClient(host, port, caCertPath, keyPath, certPath)
	if err != nil {
		return err
	}

	datasetID, err := c.CreateVolume(dir)
	if err != nil {
		return err
	}

	b.flocker.datasetID = datasetID
	return nil
}

func (b flockerBuilder) IsReadOnly() bool {
	return b.readOnly
}

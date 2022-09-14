/*
Copyright 2022 The Volcano Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
	v1alpha1 "volcano.sh/apis/pkg/apis/bus/v1alpha1"
	scheme "volcano.sh/apis/pkg/client/clientset/versioned/scheme"
)

// CommandsGetter has a method to return a CommandInterface.
// A group's client should implement this interface.
type CommandsGetter interface {
	Commands(namespace string) CommandInterface
}

// CommandInterface has methods to work with Command resources.
type CommandInterface interface {
	Create(ctx context.Context, command *v1alpha1.Command, opts v1.CreateOptions) (*v1alpha1.Command, error)
	Update(ctx context.Context, command *v1alpha1.Command, opts v1.UpdateOptions) (*v1alpha1.Command, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.Command, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.CommandList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.Command, err error)
	CommandExpansion
}

// commands implements CommandInterface
type commands struct {
	client rest.Interface
	ns     string
}

// newCommands returns a Commands
func newCommands(c *BusV1alpha1Client, namespace string) *commands {
	return &commands{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the command, and returns the corresponding command object, and an error if there is any.
func (c *commands) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.Command, err error) {
	result = &v1alpha1.Command{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("commands").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Commands that match those selectors.
func (c *commands) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.CommandList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.CommandList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("commands").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested commands.
func (c *commands) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("commands").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a command and creates it.  Returns the server's representation of the command, and an error, if there is any.
func (c *commands) Create(ctx context.Context, command *v1alpha1.Command, opts v1.CreateOptions) (result *v1alpha1.Command, err error) {
	result = &v1alpha1.Command{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("commands").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(command).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a command and updates it. Returns the server's representation of the command, and an error, if there is any.
func (c *commands) Update(ctx context.Context, command *v1alpha1.Command, opts v1.UpdateOptions) (result *v1alpha1.Command, err error) {
	result = &v1alpha1.Command{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("commands").
		Name(command.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(command).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the command and deletes it. Returns an error if one occurs.
func (c *commands) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("commands").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *commands) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("commands").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched command.
func (c *commands) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.Command, err error) {
	result = &v1alpha1.Command{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("commands").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

package resourcequota

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "resourcequota"

// resourceQuota scope not supported
type resourceQuotaPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return resourcequota plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &resourceQuotaPlugin{
		pluginArguments: arguments,
	}
}

func (rq *resourceQuotaPlugin) Name() string {
	return PluginName
}

func (rq *resourceQuotaPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.AddJobEnqueueableFn(rq.Name(), func(obj interface{}) int {
		job := obj.(*api.JobInfo)

		resourcesRequests := job.PodGroup.Spec.MinResources
		if resourcesRequests == nil {
			return util.Permit
		}

		namespaceInfo := ssn.NamespaceInfo[api.NamespaceName(job.Namespace)]
		if namespaceInfo == nil {
			return util.Abstain
		}

		quotas := namespaceInfo.QuotaStatus
		for _, resourceQuota := range quotas {
			hardResources := quotav1.ResourceNames(resourceQuota.Hard)

			used := v1.ResourceList{}
			for _, j := range ssn.Jobs {
				rr := j.PodGroup.Spec.MinResources
				if j.Namespace != job.Namespace || rr == nil {
					continue
				}

				switch j.PodGroup.Status.Phase {
				case scheduling.PodGroupRunning, scheduling.PodGroupInqueue:
					ru := quotav1.Mask(*rr, hardResources)
					used = quotav1.Add(used, ru)
				}
			}

			requestedUsage := quotav1.Mask(*resourcesRequests, hardResources)
			newUsage := quotav1.Add(used, requestedUsage)
			maskedNewUsage := quotav1.Mask(newUsage, quotav1.ResourceNames(requestedUsage))

			if allowed, exceeded := quotav1.LessThanOrEqual(maskedNewUsage, resourceQuota.Hard); !allowed {
				failedRequestedUsage := quotav1.Mask(requestedUsage, exceeded)
				failedUsed := quotav1.Mask(resourceQuota.Used, exceeded)
				failedHard := quotav1.Mask(resourceQuota.Hard, exceeded)
				msg := fmt.Sprintf("resource quota insufficient, requested: %v, used: %v, limited: %v",
					failedRequestedUsage,
					failedUsed,
					failedHard,
				)
				klog.V(4).Infof("enqueueable false for job: %s/%s, because :%s", job.Namespace, job.Name, msg)
				ssn.RecordPodGroupEvent(
					job.PodGroup,
					v1.EventTypeWarning,
					string(scheduling.PodGroupUnschedulableType),
					msg,
				)
				return util.Reject
			}
		}

		return util.Permit
	})
}

func (rq *resourceQuotaPlugin) OnSessionClose(session *framework.Session) {
}

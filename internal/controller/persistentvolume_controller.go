/*
Copyright 2023.

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

package controller

import (
	"context"
	"strconv"

	namespacedpvv1 "github.com/homirun/namespaced-pv-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PersistentVolumeReconciler reconciles a PersistentVolume object
type PersistentVolumeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumes/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumes/finalizers,verbs=update
//+kubebuilder:rbac:groups=namespaced-pv.homi.run,resources=namespacedpvs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PersistentVolume object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *PersistentVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, req.NamespacedName, pv); err != nil {
		logger.Error(err, "unable to fetch PersistentVolume")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	finalizerName := "namespacedpv.homi.run/pvFinalizer"
	if !controllerutil.ContainsFinalizer(pv, finalizerName) {
		pvCopy := pv.DeepCopy()
		pvCopy.Finalizers = append(pvCopy.Finalizers, finalizerName)
		patch := client.MergeFrom(pv)
		if err := r.Patch(ctx, pvCopy, patch); err != nil {
			logger.Error(err, "unable to patch PersistentVolume")
			return ctrl.Result{}, err
		}
	}

	r.DeletePV(ctx, pv, finalizerName)

	return ctrl.Result{}, nil
}

func (r *PersistentVolumeReconciler) DeletePV(ctx context.Context, pv *corev1.PersistentVolume, finalizerName string) error {
	logger := log.FromContext(ctx)

	if !pv.GetDeletionTimestamp().IsZero() {
		if controllerutil.ContainsFinalizer(pv, finalizerName) && pv.Annotations["pv.kubernetes.io/provisioned-by"] == "namespaced-pv-controller" {
			controllerutil.RemoveFinalizer(pv, finalizerName)
			if err := r.Update(ctx, pv); err != nil {
				logger.Error(err, "unable to update PersistentVolume")
				return err
			}

			logger.Info("pv finalizer is removed")
			namespacedPv := &namespacedpvv1.NamespacedPv{}
			r.Get(ctx, client.ObjectKey{Namespace: pv.Labels["owner-namespace"], Name: pv.Labels["owner"]}, namespacedPv)

			if namespacedPv.Annotations["namespacedpv.homi.run/recreate-pv-count"] == "" {
				namespacedPv.Annotations["namespacedpv.homi.run/recreate-pv-count"] = "0"
			}

			recreateCount, _ := strconv.Atoi(namespacedPv.Annotations["nnamespacedpv.homi.run/recreate-pv-count"])
			// FIXME: recreateCount is always 1
			namespacedPv.Annotations["namespacedpv.homi.run/recreate-pv-count"] = strconv.Itoa(recreateCount + 1)
			if err := r.Update(ctx, namespacedPv); err != nil {
				logger.Error(err, "unable to update NamespacedPv")
				return err
			}
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolume{}).
		Complete(r)
}

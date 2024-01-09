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
	"errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	namespacedpvv1 "github.com/homirun/namespaced-pv-controller/api/v1"
)

// NamespacedPvReconciler reconciles a NamespacedPv object
type NamespacedPvReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=namespaced-pv.homi.run,resources=namespacedpvs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=namespaced-pv.homi.run,resources=namespacedpvs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=namespaced-pv.homi.run,resources=namespacedpvs/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumes/finalizers,verbs=update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NamespacedPv object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.4/pkg/reconcile
func (r *NamespacedPvReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var namespacedPv namespacedpvv1.NamespacedPv
	if err := r.Get(ctx, req.NamespacedName, &namespacedPv); err != nil {
		logger.Error(err, "unable to fetch NamespacedPv")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	err := r.CreateOrUpdatePv(ctx, &namespacedPv)
	if err != nil {
		logger.Error(err, "unable to create PersistentVolume")
		return ctrl.Result{}, err
	}

	finalizerName := "namespacedpv.homi.run/finalizer"
	if !controllerutil.ContainsFinalizer(&namespacedPv, finalizerName) {
		namespacedPvCopy := namespacedPv.DeepCopy()
		namespacedPvCopy.Finalizers = []string{finalizerName}
		patch := client.MergeFrom(&namespacedPv)
		if err := r.Patch(ctx, namespacedPvCopy, patch); err != nil {
			logger.Error(err, "unable to patch NamespacedPv")
			return ctrl.Result{}, err
		}
	}

	var pvLists corev1.PersistentVolumeList
	if err = r.List(ctx, &pvLists, &client.ListOptions{LabelSelector: labels.SelectorFromSet(map[string]string{"owner": namespacedPv.Name})}); err != nil {
		logger.Error(err, "unable to fetch PersistentVolume")
		return ctrl.Result{}, err
	}

	for _, pv := range pvLists.Items {
		err = r.DeleteNamespacedPV(ctx, &namespacedPv, &pv, finalizerName)
		if err != nil {
			logger.Error(err, "unable to delete NamespacedPv")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespacedPvReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&namespacedpvv1.NamespacedPv{}).
		Complete(r)
}

// create PersietentVolume
func (r *NamespacedPvReconciler) CreateOrUpdatePv(ctx context.Context, namespacedPv *namespacedpvv1.NamespacedPv) error {
	// only nfs supported
	logger := log.FromContext(ctx)
	if &namespacedPv.Spec.Nfs == nil {
		nilNfsError := errors.New("only nfs supported")
		return nilNfsError
	}

	// create PersistentVolume
	pv := &corev1.PersistentVolume{}

	pv.SetName(namespacedPv.Spec.VolumeName + "-" + namespacedPv.Namespace)
	pv.SetLabels(map[string]string{
		"owner":           namespacedPv.Name,
		"owner-namespace": namespacedPv.Namespace,
	})
	pv.SetAnnotations(map[string]string{
		"pv.kubernetes.io/provisioned-by": "namespaced-pv-controller",
	})

	op, err := controllerutil.CreateOrPatch(ctx, r.Client, pv, func() error {
		if namespacedPv.Spec.ClaimRefName != "" {
			if pv.Spec.ClaimRef == nil {
				pv.Spec = corev1.PersistentVolumeSpec{
					AccessModes: namespacedPv.Spec.AccessModes,
					Capacity:    namespacedPv.Spec.Capacity,
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						NFS: &corev1.NFSVolumeSource{
							Server:   namespacedPv.Spec.Nfs.Server,
							Path:     namespacedPv.Spec.Nfs.Path,
							ReadOnly: namespacedPv.Spec.Nfs.ReadOnly,
						},
					},
					PersistentVolumeReclaimPolicy: namespacedPv.Spec.ReclaimPolicy,
					StorageClassName:              namespacedPv.Spec.StorageClassName,
					VolumeMode:                    &namespacedPv.Spec.VolumeMode,
					ClaimRef: &corev1.ObjectReference{
						Namespace: namespacedPv.Namespace,
						Name:      namespacedPv.Spec.ClaimRefName,
					},
				}
			} else {
				newPvClaimRef := pv.Spec.ClaimRef.DeepCopy()
				pv.Spec = corev1.PersistentVolumeSpec{
					AccessModes: namespacedPv.Spec.AccessModes,
					Capacity:    namespacedPv.Spec.Capacity,
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						NFS: &corev1.NFSVolumeSource{
							Server:   namespacedPv.Spec.Nfs.Server,
							Path:     namespacedPv.Spec.Nfs.Path,
							ReadOnly: namespacedPv.Spec.Nfs.ReadOnly,
						},
					},
					PersistentVolumeReclaimPolicy: namespacedPv.Spec.ReclaimPolicy,
					StorageClassName:              namespacedPv.Spec.StorageClassName,
					VolumeMode:                    &namespacedPv.Spec.VolumeMode,
				}
				pv.Spec.ClaimRef = newPvClaimRef

				pv.Spec.ClaimRef.Namespace = namespacedPv.Namespace
				pv.Spec.ClaimRef.Name = namespacedPv.Spec.ClaimRefName
			}

		} else {
			pv.Spec = corev1.PersistentVolumeSpec{
				AccessModes: namespacedPv.Spec.AccessModes,
				Capacity:    namespacedPv.Spec.Capacity,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					NFS: &corev1.NFSVolumeSource{
						Server:   namespacedPv.Spec.Nfs.Server,
						Path:     namespacedPv.Spec.Nfs.Path,
						ReadOnly: namespacedPv.Spec.Nfs.ReadOnly,
					},
				},
				PersistentVolumeReclaimPolicy: namespacedPv.Spec.ReclaimPolicy,
				StorageClassName:              namespacedPv.Spec.StorageClassName,
				VolumeMode:                    &namespacedPv.Spec.VolumeMode,
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("PersistentVolume created or patched", "operation", op)
		r.UpdateStatus(ctx, namespacedPv)
	}

	return nil
}

func (r *NamespacedPvReconciler) DeleteNamespacedPV(ctx context.Context, namespacedPv *namespacedpvv1.NamespacedPv, targetPv *corev1.PersistentVolume, finalizerName string) error {
	logger := log.FromContext(ctx)
	if !namespacedPv.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(namespacedPv, finalizerName) && targetPv.Annotations["pv.kubernetes.io/provisioned-by"] == "namespaced-pv-controller" {
			controllerutil.RemoveFinalizer(namespacedPv, finalizerName)
			if err := r.Update(ctx, namespacedPv); err != nil {
				logger.Error(err, "unable to update NamespacedPv")
				return err
			}
			if controllerutil.ContainsFinalizer(targetPv, "namespacedpv.homi.run/pvFinalizer") {
				controllerutil.RemoveFinalizer(targetPv, "namespacedpv.homi.run/pvFinalizer")
				if err := r.Update(ctx, targetPv); err != nil {
					logger.Error(err, "unable to update PersistentVolume")
					return err
				}
				r.Get(ctx, types.NamespacedName{Name: targetPv.Name}, targetPv)
				if err := r.Delete(ctx, targetPv); err != nil {
					logger.Error(err, "unable to delete NamespacedPv")
					return err
				}
			}
		}
		return nil
	}
	return nil
}

func (r *NamespacedPvReconciler) UpdateStatus(ctx context.Context, namespacedPv *namespacedpvv1.NamespacedPv) error {
	logger := log.FromContext(ctx)
	newNamespacedPv := namespacedPv.DeepCopy()
	newNamespacedPv.Status.RefPvName = namespacedPv.Spec.VolumeName + "-" + namespacedPv.Namespace
	newNamespacedPv.Status.RefPvUid, _ = r.GetPvUid(ctx, namespacedPv)
	patch := client.MergeFrom(namespacedPv)
	if err := r.Status().Patch(ctx, newNamespacedPv, patch); err != nil {
		logger.Error(err, "unable to update NamespacedPv status")
		return err
	}
	return nil
}

func (r *NamespacedPvReconciler) GetPvUid(ctx context.Context, namespacedPv *namespacedpvv1.NamespacedPv) (string, error) {
	logger := log.FromContext(ctx)
	pv := &corev1.PersistentVolume{}
	if err := r.Get(ctx, types.NamespacedName{Name: namespacedPv.Spec.VolumeName + "-" + namespacedPv.Namespace}, pv); err != nil {
		logger.Error(err, "unable to get PersistentVolume")
		return "", err
	}
	return string(pv.UID), nil
}

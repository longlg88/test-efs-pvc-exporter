#!/usr/bin/python
import os
import sys
from subprocess import Popen, PIPE, STDOUT
import subprocess
import timeit
import json
from datetime import datetime
from threading import Thread

start = timeit.default_timer() # Record processing time

def human_bytes(B):
    """Return human readable file unit like KB, MB, GB string by Byte
    Args:
        B : input byte values
    Returns:
        KB / MB / GB
    """
    B = float(B)
    KB = float(1024)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.2f} KB'.format(B/KB)
    elif MB <= B < GB:
        return '{0:.2f} MB'.format(B/MB)
    elif GB <= B < TB:
        return '{0:.2f} GB'.format(B/GB)

# def get_pod_info():
#     """Return pod name, claim name in kubernetes cluster for using get_pvc_info()
#     kubectl get pv -A
#     """

def get_pvc_info():
    """Return namespace, pv name, volumeName for filtering PVC in kubernetes cluster
    Filter condition
        - Is it Bound?
        - Is it StorageClass efs?
        - 
    """

    info_pvc_cmd = "kubectl get pvc --all-namespaces -o json | jq -r '.items[] | select( ( .spec.storageClassName | contains(" + '"' + "efs" + '"' + ") ) and ( .status.phase | contains(" + '"' + "Bound" + '"' + ") ) )' | jq -r '.metadata.namespace, .metadata.name, .spec.volumeName'"
    info_pvc = Popen(info_pvc_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    info_pvc_list = info_pvc.stdout.read().split()
    count=3
    info_pvc_list = [ info_pvc_list[i:i+count] for i in range(0,len(info_pvc_list),count) ]
        
    return info_pvc_list

def get_efs_provisioner():
    efs_provisioner_cmd = "kubectl get pod -n kube-system | grep efs | awk '{print $1}'"
    efs_provisioner_res = Popen(efs_provisioner_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    efs_provisioner_id = efs_provisioner_res.stdout.read().replace('\n','')
    return efs_provisioner_id

def get_pv_name():
    """Return pv name in kubernetes cluster
    """
    
    pv_id_cmd = "kubectl exec -it "+get_efs_provisioner()+ " -n kube-system -- ls -al /persistentvolumes | awk '{print $9}' | sed '1,3d'"
    pv_id_res = Popen(pv_id_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    pv_id_list = pv_id_res.stdout.readlines()[1:]

    return pv_id_list

def match_collect_info():
    """Return volume size matching pv id and efs directory's name which is exactly same.
    """
    info_list=get_pvc_info()
    pv_list=get_pv_name()

    size_pvc=[]
    metric_list = []
    datetime_now = datetime.now() # Define current time

    for pv_name in pv_list:
        for i_group in info_list:
            i_name=i_group[1]+'-'+i_group[2]
            pv_name=pv_name.replace('\n','')
            if pv_name == i_name:
                # Calculate volume size
                m_size_cmd = "kubectl exec -it "+get_efs_provisioner()+" -n kube-system -- du -ks /persistentvolumes/" + pv_name.replace('\n','') + " | awk '{print $1}'"
                m_size_res = Popen(m_size_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
                m_size = m_size_res.stdout.readlines()[1:]
                sum_size = human_bytes(int(m_size[0].replace('\n',''))*1024)
                size_pvc.append(sum_size)

                # Find pod name for using claim name
                find_pod_name_cmd = "kubectl get pod -n "+ i_group[0] +" | grep "+ i_group[1] + " | awk '{print $1}'"
                find_pod_name_res = Popen(find_pod_name_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
                find_pod_name = find_pod_name_res.stdout.read().replace('\n','')
                
                if find_pod_name and find_pod_name != "No resources found.":
                    metric_info = {"namespace":i_group[0], "name":find_pod_name, "size":str(sum_size), "pvc":pv_name}
                    metric_list.append(metric_info)
    
    json_info = {"timestamp":str(datetime_now),"metadata":{ "pod":metric_list } } # Before change json type
    #print(json_info)
    return json_info    

def all_efs_collect_info():
    """Return all efs directory volumes size
    It needs to calculate all pvc which isn't detecting 'match_collect_info' function.
    Because there are not usable pvc's in there and we need to find and destroy.
    """
    pv_list=get_pv_name()

    size_pvc = []
    metric_list = []
    datetime_now = datetime.now()

    for pv_name in pv_list:
        all_size_cmd = "kubectl exec -it "+get_efs_provisioner()+" -n kube-system -- du -ks /persistentvolumes/"+pv_name.replace('\n','')+ " | awk '{print $1}'"
        all_size_res = Popen(all_size_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        all_size = all_size_res.stdout.readlines()[1:]
        all_sum_size = human_bytes(int(all_size[0].replace('\n',''))*1024)
        size_pvc.append(all_sum_size)

        metric_info = {"pvc":pv_name, "size":str(all_sum_size)}
        metric_list.append(metric_info)

    json_info = {"timestamp":str(datetime_now),"metadata":{ "pod":metric_list } } # Before change json type
    # print(json_info)
    return json_info

if __name__ == "__main__":
    json_info = match_collect_info()
    all_json_info = all_efs_collect_info()
    #th_json_info=Thread(target=match_collect_info, args=())
    #th_json_info.start()
    #th_all_json_info=Thread(target=all_efs_collect_info, args=())
    #th_all_json_info.start()

    print(json.dumps(json_info))
    print(json.dumps(all_json_info))

    print('\nlaptime = ' % (timeit.default_timer() - start))
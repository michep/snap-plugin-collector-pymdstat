import socket
import time
import mdstat
import snap_plugin.v1 as snap


class MdstatCollector(snap.Collector):

    def __init__(self, *args):
        self.hostname = socket.gethostname().lower()
        super(MdstatCollector, self).__init__(*args)

    def update_catalog(self, config):
        metrics = []
        metric = snap.Metric(version=1)
        metric.namespace.add_static_element("mfms")                              # /0
        metric.namespace.add_static_element("mdstat")                            # /1
        metric.namespace.add_dynamic_element("raid", "raid device name")         # /2
        metric.namespace.add_dynamic_element("disk", "raid disk name")           # /3
        metric.namespace.add_static_element("health")                            # /4
        metrics.append(metric)

        return metrics

    def collect(self, metrics):
        metrics_return = []
        ts_now = time.time()
        md_devs = mdstat.parse()['devices']
        for (md_dev_name, md_dev_info) in md_devs.items():
            metric = snap.Metric(namespace=[i for i in metrics[0].namespace])
            metric.namespace[2].value = md_dev_name
            metric.namespace[3].value = "DISKS"
            metric.data = md_dev_info["status"]["raid_disks"] - len(md_dev_info["disks"])
            metric.timestamp = ts_now
            metric.tags['host'] = self.hostname
            metrics_return.append(metric)
            for (disk_name, disk_info) in md_dev_info['disks'].items():
                metric = snap.Metric(namespace=[i for i in metrics[0].namespace])
                metric.namespace[2].value = md_dev_name
                metric.namespace[3].value = disk_name
                metric.data = 1 if disk_info['faulty'] else 0
                metric.timestamp = ts_now
                metric.tags['host'] = self.hostname
                metrics_return.append(metric)

        return metrics_return

    def get_config_policy(self):
        return snap.ConfigPolicy()

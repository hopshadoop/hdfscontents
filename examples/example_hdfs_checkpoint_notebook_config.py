from hdfscontents.hdfscheckpoints import HDFSCheckpoints

# Tell IPython to use HDFSCheckpoints for checkpoint storage.
c.ContentsManager.checkpoints_class = HDFSCheckpoints

c.HDFSCheckpoints.hdfs_namenode_host='<namenode_host_ip>'
c.HDFSCheckpoints.hdfs_namenode_port=8020
c.HDFSCheckpoints.root_dir='/user/amangarg'
c.HDFSCheckpoints.hdfs_user='hdfs'

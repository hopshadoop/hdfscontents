from hdfscontents.hdfsmanager import HDFSContentsManager

# Tell IPython to use HDFSContentsManager as ContentsManager .
c.NotebookApp.contents_manager_class = HDFSContentsManager

c.HDFSCheckpoints.hdfs_namenode_host='<namenode_host_ip>'
c.HDFSCheckpoints.hdfs_namenode_port=8020
c.HDFSCheckpoints.root_dir='/user/hdfs_user'
c.HDFSCheckpoints.hdfs_user='hdfs'

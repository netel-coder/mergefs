#基于FUSE的用户态文件系统。
- 支持固态和机械硬盘合并挂载，写入时大小分流。
- 小文件写固态，大文件写机械盘，提供统一写入读取挂载点。
- 支持指定文件大小阈值:`size_th`,单位为`KB`

#使用方法
> python3 merge.py -h

> python3 merge.py /mnt/ssd /mnt/hdd /mnt/merged --size_th=1024 --debug=1

> 依赖:pip install fusepy

- 已有文件系统请使用size_fix.py完成大小文件分拣。
> python3 size_fix.py /mnt/ssd /mnt/hdd --size_th=1024


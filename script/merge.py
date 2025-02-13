import os
import errno
import argparse
import threading
from fusepy import FUSE, Operations
import time


class SplitFileSystem(Operations):
    def __init__(self, ssd_path, hdd_path, size_th, debug=False):
        self.ssd_path = ssd_path
        self.hdd_path = hdd_path
        self.lock_th = threading.Lock()  # 线程锁，确保线程安全
        self.size_th = size_th * 1024
        self.debug = debug

    def get_target_path(self, path, size=0):
        """根据文件大小决定文件存储路径"""
        if size < self.size_th:  # 小于 4MB 的文件存储在 SSD
            return os.path.join(self.ssd_path, path.lstrip('/'))
        else:  # 大于等于 4MB 的文件存储在 HDD
            return os.path.join(self.hdd_path, path.lstrip('/'))

    def create(self, path, mode, fi=None):
        self.debug and print("create", path)
        """创建文件"""
        with self.lock_th:
            # 先默认创建在SSD上
            target_path = os.path.join(self.ssd_path, path.lstrip('/'))
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            target_path = os.path.normpath(target_path)  # 规范化路径
            return os.open(target_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    def open(self, path, flags):
        self.debug and print("open", path)
        """打开文件"""
        with self.lock_th:
            # 获取文件实际大小
            ssd_path = os.path.join(self.ssd_path, path.lstrip('/'))
            hdd_path = os.path.join(self.hdd_path, path.lstrip('/'))
            if os.path.exists(ssd_path):
                target_path = ssd_path
            elif os.path.exists(hdd_path):
                target_path = hdd_path
            else:
                raise OSError(errno.ENOENT, "File not found", path)
            self.debug and print("open:", target_path)
            return os.open(target_path, flags)

    def write(self, path, data, offset, fh):
        """写入文件"""
        with self.lock_th:
            ssd_path = os.path.join(self.ssd_path, path.lstrip('/'))
            hdd_path = os.path.join(self.hdd_path, path.lstrip('/'))
            # 获取当前文件大小
            if os.path.exists(ssd_path):
                current_size = os.path.getsize(ssd_path)
            elif os.path.exists(hdd_path):
                current_size = os.path.getsize(hdd_path)
            else:
                current_size = 0
            new_size = current_size + len(data)

            # 判断是否需要移动文件
            if current_size < self.size_th and new_size >= self.size_th:
                if os.path.exists(ssd_path):
                    self.debug and print("moved from SSD to HDD", path)
                    # 从SSD移动到HDD
                    os.close(fh)
                    with open(ssd_path, 'rb') as f:
                        content = f.read()
                    os.unlink(ssd_path)
                    os.makedirs(os.path.dirname(hdd_path), exist_ok=True)
                    fh = os.open(hdd_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                    os.write(fh, content)
            os.lseek(fh, offset, os.SEEK_SET)
            return os.write(fh, data)

    def read(self, path, size, offset, fh):
        """读取文件"""
        with self.lock_th:
            os.lseek(fh, offset, os.SEEK_SET)
            return os.read(fh, size)

    def flush(self, path, fh):
        self.debug and print("flush", path)
        """刷新文件缓冲区"""
        with self.lock_th:
            os.fsync(fh)

    def release(self, path, fh):
        self.debug and print("release", path)
        """关闭文件"""
        with self.lock_th:
            os.close(fh)
            ssd_path = os.path.join(self.ssd_path, path.lstrip('/'))
            hdd_path = os.path.join(self.hdd_path, path.lstrip('/'))

            # 检查文件是否在 HDD 上
            if os.path.exists(hdd_path):
                file_size = os.path.getsize(hdd_path)
                # 判断是否需要从 HDD 移动到 SSD
                if file_size < 4 * 1024 * 1024:
                    self.debug and print("moved from HDD to SSD", path)
                    try:
                        # 读取 HDD 上的文件内容
                        with open(hdd_path, 'rb') as hdd_file:
                            content = hdd_file.read()
                        # 删除 HDD 上的文件
                        os.unlink(hdd_path)
                        # 创建 SSD 上的目录（如果不存在）
                        os.makedirs(os.path.dirname(ssd_path), exist_ok=True)
                        # 将内容写入 SSD 上的文件
                        with open(ssd_path, 'wb') as ssd_file:
                            ssd_file.write(content)
                    except Exception as e:
                        self.debug and print(f"Error moving file from HDD to SSD: {e}")

    def fsync(self, path, datasync, fh):
        self.debug and print("fsync", path)
        """同步文件数据到磁盘"""
        with self.lock_th:
            if datasync:
                os.fdatasync(fh)
            else:
                os.fsync(fh)

    def truncate(self, path, length, fh=None):
        self.debug and print("truncate", path)
        """截断文件"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                with open(target_path_ssd, 'r+b') as f:
                    f.truncate(length)
            if os.path.exists(target_path_hdd):
                with open(target_path_hdd, 'r+b') as f:
                    f.truncate(length)

    def unlink(self, path):
        self.debug and print("unlink", path)
        """删除文件"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                os.unlink(target_path_ssd)
            if os.path.exists(target_path_hdd):
                os.unlink(target_path_hdd)

    def mkdir(self, path, mode):
        """创建目录"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            os.makedirs(target_path_ssd, mode=0o777, exist_ok=True)  # 确保目录可写
            os.makedirs(target_path_hdd, mode=0o777, exist_ok=True)  # 确保目录可写

    def rmdir(self, path):
        """删除目录"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                os.rmdir(target_path_ssd)
            if os.path.exists(target_path_hdd):
                os.rmdir(target_path_hdd)

    def readdir(self, path, fh):
        """读取目录内容"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            files = set()

            if os.path.exists(target_path_ssd):
                files.update(os.listdir(target_path_ssd))
            if os.path.exists(target_path_hdd):
                files.update(os.listdir(target_path_hdd))

            return ['.', '..'] + list(files)

    def rename(self, old_path, new_path):
        self.debug and print("rename", old_path, new_path)
        """在挂载点内移动文件或目录(重命名)"""
        with self.lock_th:
            old_path_ssd = os.path.normpath(os.path.join(self.ssd_path, old_path.lstrip('/')))
            new_path_ssd = os.path.normpath(os.path.join(self.ssd_path, new_path.lstrip('/')))

            old_path_hdd = os.path.normpath(os.path.join(self.hdd_path, old_path.lstrip('/')))
            new_path_hdd = os.path.normpath(os.path.join(self.hdd_path, new_path.lstrip('/')))

            # 如果SSD存在旧路径
            if os.path.exists(old_path_ssd):
                os.rename(old_path_ssd, new_path_ssd)

                # 如果新路径也在 SSD
                # if self.get_target_path(new_path, os.path.getsize(old_path_ssd)) == new_path_ssd:
                #     os.rename(old_path_ssd, new_path_ssd)
                # else:  # 跨设备重命名（SSD -> HDD）
                #     with open(old_path_ssd, 'rb') as f:
                #         content = f.read()
                #     os.unlink(old_path_ssd)
                #     with open(new_path_hdd, 'wb') as f:
                #         f.write(content)

            # 如果旧路径在 HDD
            if os.path.exists(old_path_hdd):
                os.rename(old_path_hdd, new_path_hdd)
                # 如果新路径也在 HDD
                # if self.get_target_path(new_path, os.path.getsize(old_path_hdd)) == new_path_hdd:
                #     os.rename(old_path_hdd, new_path_hdd)
                # else:  # 跨设备重命名（HDD -> SSD）
                #     with open(old_path_hdd, 'rb') as f:
                #         content = f.read()
                #     os.unlink(old_path_hdd)
                #     with open(new_path_ssd, 'wb') as f:
                #         f.write(content)
            # else:
            #     raise OSError(errno.ENOENT, "File not found", old_path)

    def getattr(self, path, fh=None):
        self.debug and print("getattr", path)
        """获取文件属性"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                st = os.lstat(target_path_ssd)
            elif os.path.exists(target_path_hdd):
                st = os.lstat(target_path_hdd)
            else:
                raise OSError(errno.ENOENT, "File not found", path)

            # 确保返回的文件模式包含写权限
            attrs = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                             'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                             'st_uid'))
            attrs['st_mode'] = attrs['st_mode'] | 0o222  # 添加写权限
            return attrs

    def chmod(self, path, mode):
        """更改文件权限"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                os.chmod(target_path_ssd, mode)
            if os.path.exists(target_path_hdd):
                os.chmod(target_path_hdd, mode)

    def chown(self, path, uid, gid):
        """更改文件所有者"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                os.chown(target_path_ssd, uid, gid)
            if os.path.exists(target_path_hdd):
                os.chown(target_path_hdd, uid, gid)

    def utimens(self, path, times=None):
        """更改文件时间戳"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if times is None:
                times = (time.time(), time.time())
            if os.path.exists(target_path_ssd):
                os.utime(target_path_ssd, times)
            if os.path.exists(target_path_hdd):
                os.utime(target_path_hdd, times)

    def statfs(self, path):
        self.debug and print("statfs", path)
        """获取文件系统统计信息"""
        with self.lock_th:
            ssd_stat = os.statvfs(self.ssd_path)
            hdd_stat = os.statvfs(self.hdd_path)

            return {
                'f_bsize': ssd_stat.f_bsize,  # 文件系统块大小
                'f_blocks': ssd_stat.f_blocks + hdd_stat.f_blocks,  # 总块数
                'f_bfree': ssd_stat.f_bfree + hdd_stat.f_bfree,  # 空闲块数
                'f_bavail': ssd_stat.f_bavail + hdd_stat.f_bavail,  # 可用块数
                'f_files': ssd_stat.f_files + hdd_stat.f_files,  # 总文件数
                'f_ffree': ssd_stat.f_ffree + hdd_stat.f_ffree,  # 空闲文件数
            }

    def access(self, path, mode):
        self.debug and print("access", path)
        """检查文件或目录的访问权限"""
        with self.lock_th:
            target_path_ssd = os.path.normpath(os.path.join(self.ssd_path, path.lstrip('/')))
            target_path_hdd = os.path.normpath(os.path.join(self.hdd_path, path.lstrip('/')))
            if os.path.exists(target_path_ssd):
                if not os.access(target_path_ssd, mode):
                    raise OSError(errno.EACCES, "Permission denied", path)
            elif os.path.exists(target_path_hdd):
                if not os.access(target_path_hdd, mode):
                    raise OSError(errno.EACCES, "Permission denied", path)
            else:
                raise OSError(errno.ENOENT, "File not found", path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Mount a split file system using FUSE. \n Usage: python3 merge.py /mnt/ssd /mnt/hdd /mnt/merged --size_th=1024 --debug=1")
    parser.add_argument('ssd_path', help='Path to the SSD storage,eg:/mnt/ssd')
    parser.add_argument('hdd_path', help='Path to the HDD storage,eg:/mnt/hdd')
    parser.add_argument('mount_point', help='Path where the file system will be mounted,eg:/mnt/merged')
    parser.add_argument('--size_th', type=int, default=1024,
                        help='small file threshold,default 1024.If the value is too high, it will consume too much SSD lifespan.')
    parser.add_argument('--debug', help='print debug info,debug or empty')
    args = parser.parse_args()

    print("ssd:",args.ssd_path)
    print("hdd:",args.hdd_path)
    print("size_th:",args.size_th)
    print("debug:",args.debug)
    print("merge start")
    # 挂载文件系统
    fuse = FUSE(
        SplitFileSystem(args.ssd_path, args.hdd_path, args.size_th, args.debug),
        args.mount_point,
        foreground=True,
        nothreads=False,
        allow_other=True,
        big_writes=True,  # 降低大文件上下文小号
        direct_io=True,  # 提升速度
        ro=False  # 确保文件系统是可写的
    )
    print("merge exit")

import os
import shutil
import argparse


def get_file_size(path):
    """获取文件大小，单位为 KB"""
    return os.path.getsize(path) / 1024


def traverse_and_move(source_dir, target_dir, size_threshold, compare_func):
    """遍历源目录，根据文件大小和比较函数移动文件到目标目录"""
    file_count = 0
    folder_count = 0
    total_size_kb = 0
    moved_file_count = 0
    moved_total_size_kb = 0

    for root, dirs, files in os.walk(source_dir):
        for dir in dirs:
            folder_count += 1
        for file in files:
            file_path = os.path.join(root, file)
            file_size = get_file_size(file_path)
            total_size_kb += file_size
            file_count += 1

            if compare_func(file_size, size_threshold):
                relative_path = os.path.relpath(root, source_dir)
                target_suHDD_DIR = os.path.join(target_dir, relative_path)
                if not os.path.exists(target_suHDD_DIR):
                    os.makedirs(target_suHDD_DIR)
                    # 保持文件夹权限
                    shutil.copystat(root, target_suHDD_DIR)
                target_file_path = os.path.join(target_suHDD_DIR, file)
                # 移动文件
                shutil.move(file_path, target_file_path)
                moved_file_count += 1
                moved_total_size_kb += file_size

    # 将 KB 转换为 GB
    total_size_gb = total_size_kb / (1024 * 1024)
    moved_total_size_gb = moved_total_size_kb / (1024 * 1024)

    return file_count, folder_count, total_size_gb, moved_file_count, moved_total_size_gb


def main():
    parser = argparse.ArgumentParser(
        description="Move file by size \n Usage: python3 size_fix.py /mnt/ssd /mnt/hdd --size_th=1024")
    parser.add_argument('ssd_path', help='Path to the SSD storage,eg:/mnt/ssd')
    parser.add_argument('hdd_path', help='Path to the HDD storage,eg:/mnt/hdd')
    parser.add_argument('--size_th', type=int, default=1024,
                        help='small file threshold,default 1024(KB).If the value is too high, it will consume too much SSD lifespan.')
    args = parser.parse_args()
    SSD_DIR = args.ssd_path
    HDD_DIR = args.hdd_path
    size_threshold = args.size_th

    # 遍历 A 文件夹，将大于阈值的文件移动到 B 文件夹
    print("正在遍历SSD文件夹...")
    A_file_count, A_folder_count, A_total_size, A_moved_file_count, A_moved_total_size = traverse_and_move(
        SSD_DIR, HDD_DIR, size_threshold, lambda x, y: x > y)
    print(f"SS文件夹操作信息：")
    print(f"  遍历的文件数: {A_file_count}")
    print(f"  遍历的文件夹数: {A_folder_count}")
    print(f"  遍历的总容量: {A_total_size:.6f} GB")
    print(f"  移至HDD的文件数: {A_moved_file_count}")
    print(f"  移动的总容量: {A_moved_total_size:.6f} GB")

    # 遍历 B 文件夹，将小于阈值的文件移动到 A 文件夹
    print("正在遍历HDD文件夹...")
    B_file_count, B_folder_count, B_total_size, B_moved_file_count, B_moved_total_size = traverse_and_move(
        HDD_DIR, SSD_DIR, size_threshold, lambda x, y: x < y)
    print(f"HDD文件夹操作信息：")
    print(f"  遍历的文件数: {B_file_count}")
    print(f"  遍历的文件夹数: {B_folder_count}")
    print(f"  遍历的总容量: {B_total_size:.6f} GB")
    print(f"  移至SSD的文件数: {B_moved_file_count}")
    print(f"  移动的总容量: {B_moved_total_size:.6f} GB")


if __name__ == "__main__":
    main()
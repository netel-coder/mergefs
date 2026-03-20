#!/bin/bash

# 检查是否提供了目录路径作为参数
if [ $# -ne 1 ]; then
    echo "用法: $0 <目录路径>"
    exit 1
fi

# 获取输入的目录路径
directory=$1

# 检查输入的路径是否为有效的目录
if [ ! -d "$directory" ]; then
    echo "输入的路径不是有效的目录。"
    exit 1
fi

# 初始化各个区间的文件数量和占用空间总和
declare -A small_file_counts
declare -A small_file_spaces
declare -A large_file_counts
declare -A large_file_spaces

# 定义不同的界限（单位：字节）
limits=(1048576 4194304 8388608 16777216 67108864 134217728)
limit_names=("1MB" "4MB" "8MB" "16MB" "64MB" "128MB")

# 初始化每个界限的计数器和空间总和
for limit in "${limits[@]}"; do
    small_file_counts[$limit]=0
    small_file_spaces[$limit]=0
    large_file_counts[$limit]=0
    large_file_spaces[$limit]=0
done

# 遍历目录下的所有文件
while IFS= read -r -d '' file; do
    file_size=$(stat -c%s "$file")
    for limit in "${limits[@]}"; do
        if [ $file_size -lt $limit ]; then
            ((small_file_counts[$limit]++))
            ((small_file_spaces[$limit]+=file_size))
        else
            ((large_file_counts[$limit]++))
            ((large_file_spaces[$limit]+=file_size))
        fi
    done
done < <(find "$directory" -type f -print0)

# 输出统计结果
for i in "${!limits[@]}"; do
    limit=${limits[$i]}
    small_space_gb=$(echo "scale=2; ${small_file_spaces[$limit]} / (1024 * 1024 * 1024)" | bc)
    large_space_gb=$(echo "scale=2; ${large_file_spaces[$limit]} / (1024 * 1024 * 1024)" | bc)
    echo "小于 ${limit_names[$i]} 文件数量: ${small_file_counts[$limit]}"
    echo "小于 ${limit_names[$i]} 文件占用空间总和: $small_space_gb GB"
    echo "大于等于 ${limit_names[$i]} 文件数量: ${large_file_counts[$limit]}"
    echo "大于等于 ${limit_names[$i]} 文件占用空间总和: $large_space_gb GB"
    echo
done

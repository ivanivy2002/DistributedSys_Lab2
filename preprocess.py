import json

import jieba
import re


def process_dat_file_utf8(input_file, output_file):
    with open(input_file, 'r', encoding='gb18030') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            doc_unit = []
            for line in infile:
                line = line.strip()
                doc_unit.append(line)

                # 判断是否读取了一个完整的 <doc> 单位
                if len(doc_unit) == 6:
                    content = extract_content(doc_unit)
                    outfile.write(content + '\n')
                    doc_unit = []  # 清空单位列表

def process_dat_file_gbk(input_file, output_file):
    with open(input_file, 'r', encoding='gb18030') as infile:
        with open(output_file, 'w', encoding='gb18030') as outfile:
            doc_unit = []
            for line in infile:
                line = line.strip()
                doc_unit.append(line)

                # 判断是否读取了一个完整的 <doc> 单位
                if len(doc_unit) == 6:
                    content = extract_content(doc_unit)
                    outfile.write(content + '\n')
                    doc_unit = []  # 清空单位列表
def process_dat_file_gbk_json(input_file, output_file):
    with open(input_file, 'r', encoding='gb18030') as infile:
        with open(output_file, 'w', encoding='gb18030') as outfile:
            doc_unit = []
            for line in infile:
                line = line.strip()
                doc_unit.append(line)

                # 判断是否读取了一个完整的 <doc> 单位
                if len(doc_unit) == 6:
                    content = extract_content_json(doc_unit)
                    outfile.write(content + '\n')
                    doc_unit = []  # 清空单位列表


def extract_content(doc_unit):
    # 从 <content> 标签中提取内容
    docno_match = re.search(r'<docno>(.*?)</docno>', '\n'.join(doc_unit), re.DOTALL)
    contenttitle_match = re.search(r'<contenttitle>(.*?)</contenttitle>', '\n'.join(doc_unit), re.DOTALL)
    content_match = re.search(r'<content>(.*?)</content>', '\n'.join(doc_unit), re.DOTALL)
    if content_match or contenttitle_match:
        return docno_match.group(1).strip() + "\t" + contenttitle_match.group(1).strip() + "\t" + content_match.group(
            1).strip()
    else:
        return ''

def extract_content_json(doc_unit):
    docno_match = re.search(r'<docno>(.*?)</docno>', '\n'.join(doc_unit), re.DOTALL)
    contenttitle_match = re.search(r'<contenttitle>(.*?)</contenttitle>', '\n'.join(doc_unit), re.DOTALL)
    content_match = re.search(r'<content>(.*?)</content>', '\n'.join(doc_unit), re.DOTALL)

    data = {
        'docno': docno_match.group(1).strip(),
        'contenttitle': contenttitle_match.group(1).strip() if contenttitle_match else '',
        'content': content_match.group(1).strip() if content_match else ''
    }

    return json.dumps(data, ensure_ascii=False)

def main():
    # 调用函数处理文档
    input_file = 'data/document.dat'
    output_file = 'data/document_utf8.txt'
    process_dat_file_utf8(input_file, output_file)
    output_file = 'data/document_gbk.txt'
    process_dat_file_gbk(input_file, output_file)
    output_file = 'data/document_gbk.json'
    process_dat_file_gbk_json(input_file, output_file)

if "__main__" == __name__:
    main()
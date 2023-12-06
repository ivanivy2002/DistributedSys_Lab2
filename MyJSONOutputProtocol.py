import json


class MyJSONOutputProtocol(object):
    """
    自定义输出协议，补充了json的中文输出ensure_ascii=False
    """

    def read(self, line):
        k_str, v_str = line.split('\t', 1)
        return json.loads(k_str), json.loads(v_str)

    def write(self, key, value):
        return ('%s\t%s' % (json.dumps(key, ensure_ascii=False), json.dumps(value, ensure_ascii=False))).encode(
            'utf-8')

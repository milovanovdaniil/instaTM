import requests
import json
import re
import socket

def condition_lang(hashtag, lang):
    """
    Проверка принадлежности хэштега к языку
    :param hashtag: сам хэштег
    :param lang: язык
    :return ret_bool: Принадлежит хэштег языку, или нет
    """
    ret_bool = True
    if lang == 'ru':
        for i in hashtag.lower():
            if 1072 <= ord(i) <= 1103 or 32 < ord(i) <= 64:
                ret_bool = True
            else:
                ret_bool = False
                break
    else:
        for i in hashtag.lower():
            if 97 <= ord(i) <= 122 or 32 < ord(i) <= 64:
                ret_bool = True
            else:
                ret_bool = False
                break
    return ret_bool


def proxy_api():
    """
    получение списка proxy
    :return proxy_lst: возвращает список прокси
    """
    api_address = r'http://api.foxtools.ru/v2/Proxy'
    req_api = requests.get(api_address)
    d = json.loads(req_api.text)
    proxy_lst = []
    dict_types = {1: 'http', 2: 'https'}
    for item in d['response']['items']:
        try:
            proxy_lst.append({dict_types[item['type']]: f"{dict_types[item['type']]}://{item['ip']}:{item['port']}"})
        except Exception:
            pass
    return proxy_lst


def condition_camel_case(hashtag):
    """
    Возвращает True, если хэштег написан CamelCase'ом, иначе False
    :param hashtag:
    :return:
    """
    if hashtag.lower() != hashtag:
        return True
    else:
        return False


def get_edges(all_posts, hashtag):
    """
    Возвращает список со связями и список CamelCase хэштегов
    :param all_posts: список постов с хэштегом
    :param hashtag:
    :return:
    """
    global counter_breaks
    global lang
    patter = re.compile(r'#[\d\w]+\b')  # паттерн для поиска хештегов
    lst = list()
    di = dict()
    camel_case_list = list()
    for i in all_posts:
        try:
            comment = i['node']['edge_media_to_caption']['edges'][0]['node']['text']
            for j in patter.findall(comment):
                if condition_camel_case(j):
                    camel_case_list.append(j)
                if j.lower() != hashtag.lower():
                    if condition_lang(j.lower(), lang):
                        di[j] = di.get(j.lower(), 0) + 1

        except Exception:
            counter_breaks += 1
    for i in di:
        lst.append({'Source': hashtag, 'Target': i, 'Count': di[i]})
    return lst, camel_case_list


def get_info(hashtag, proxy):
    """
    Возвращает список постов по хэштегу
    :param hashtag:
    :param proxy:
    :return:
    """
    address = f'https://www.instagram.com/explore/tags/{hashtag[1:]}/?__a=1'
    r = requests.get(address, proxies=proxy)
    json_req = json.loads(r.text)
    counter = json_req['graphql']['hashtag']['edge_hashtag_to_media']['count']
    top_posts = json_req['graphql']['hashtag']['edge_hashtag_to_top_posts']['edges']
    all_posts = json_req['graphql']['hashtag']['edge_hashtag_to_media']['edges']
    all_posts.extend(top_posts)
    return counter, all_posts


def crawl(hashtag, prox):
    """
    Получение всей информации по хэштегу
    :param hashtag:
    :param prox:
    :return:
    """
    counter, all_posts = get_info(hashtag, prox)
    lst, camel_case_list = get_edges(all_posts, hashtag)
    return counter, lst, camel_case_list


def run(address, s, my_ip):
    """
    Функция, вызываемая при запуске Crawler'а
    :param address:
    :param s:
    :return:
    """
    r = s.post(address + "/REGISTER_NEW", json = {"ip": str(my_ip)})
    print(r.text)
    return json.loads((r.text))


def answer(address, s, return_json):
    """
    Функция, возвращающая ответ TaskManager'у
    :param address:
    :param s:
    :param return_json:
    :return:
    """
    r = s.post(address + "/RETURN_ANSWER", json=return_json)
    return json.loads(r.text)


if __name__ == '__main__':
    counter_breaks = 0
    s = requests.Session()
    address = 'http://127.0.0.1:9090'
    my_ip = socket.gethostbyname(socket.getfqdn())
    json_answer = run(address, s, my_ip)
    proxy_list = iter(proxy_api())
    proxy = next(proxy_list)
    lang = 'ru'
    while True:
        return_json = {}
        try:
            proxy = next(proxy_list)
        except StopIteration:
            proxy_list = iter(proxy_api())
            proxy = next(proxy_list)
        for i in json_answer:
            counter, lst, camel_case_list = crawl(i, proxy)
            return_json[i] = {'counter': counter,
                              'edges': lst}
        final_json = {"ip": my_ip, "normal": return_json, "camel_case": camel_case_list}
        json_answer = answer(address, s, final_json)

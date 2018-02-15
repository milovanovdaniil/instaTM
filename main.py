import cherrypy
import pandas as pd
from itertools import islice

@cherrypy.expose
class InstaTM(object):
    def __init__(self, address_hashtags, address_edges):
        self.json_param = {1: 2, 3: 4}
        self.crawlers = {}
        self.address_hashtags = address_hashtags
        self.address_edges = address_edges
        self.dict_values = {}
        self.first_read()

    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def REGISTER_NEW(self, **kwargs):
        input_json = cherrypy.request.json
        ip_address = input_json['ip']
        info_json = self.get_info()
        self.crawlers[ip_address] = info_json
        return self.get_info()

    def take(self, n, iterable):
        """
        Return first n items of the iterable as a list
        :param n:
        :param iterable:
        :return:
        """
        return list(islice(iterable, n))

    def first_read(self):
        self.df = pd.read_csv(self.address_hashtags, sep=';')
        self.edges_df = pd.read_csv(self.address_edges, sep=';')
        self.write_pandas_to_dict(6)

    def write_pandas_to_dict(self, index):
        for i in range(1, index):
            self.dict_values[i] = self.pandas_to_dict(i)
        else:
            self.dict_values[max(self.dict_values.keys()) + 1] = self.pandas_to_dict(max(self.dict_values.keys()) + 1,
                                                                                     tail=False)
    def pandas_to_dict(self, index, tail=True):
        dict_pandas = {}
        if tail:
            for i, row in self.edges_df.loc[self.edges_df['Count'] == index][['Count',
                                                                              'Source',
                                                                              'Target']].sort_values(by=['Count'],
                                                                                                     ascending=False).iterrows():
                if len(self.df.loc[self.df["Hashtag"] == row["Target"]]) == 0:
                    dict_pandas[row["Target"]] = row["Count"]
        else:
            for i, row in self.edges_df.loc[self.edges_df['Count'] >= index][['Count',
                                                                              'Source',
                                                                              'Target']].sort_values(by=['Count'],
                                                                                                     ascending=False).iterrows():
                if len(self.df.loc[self.df["Hashtag"] == row["Target"]]) == 0:
                    dict_pandas[row["Target"]] = row["Count"]
        return dict_pandas

    def write_info(self, json_answer):
        for k, v in json_answer.items():
            self.df = self.df.append(pd.DataFrame([{'ID': self.df.max()['ID'] + 1,
                                                    'Hashtag': k, 'Count': v['count']}]),
                                     ignore_index=True)
            self.edges_df_append(v['edges'])

        if len(self.dict_values[6]) < 100:
            self.write_pandas_to_dict(6)
    """Сохранить в CSV"""
    def get_info(self):
        return_var = self.take(100, iter(self.dict_values[6]))
        for i in return_var:
            self.dict_values[6].pop(i)
        return return_var

    def edges_change_count_value(self, source_hashtag, target_hashtag, count):
        df_for_def = self.edges_df.loc[
            ((self.edges_df['Source'] == source_hashtag) & (self.edges_df['Target'] == target_hashtag)) |
            ((self.edges_df['Source'] == target_hashtag) & (self.edges_df['Target'] == source_hashtag))]
        self.edges_df.set_value(df_for_def.index[0], 'Count', df_for_def['Count'][1] + count)

    def edges_df_append(self, list_):
        for i in list_:
            try:
                self.edges_change_count_value(i['Source'], i['Target'], i['Count'])
            except:
                self.edges_df = self.edges_df.append(pd.DataFrame([i]), ignore_index=True)


if __name__ == '__main__':
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.sessions.on': True,
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'text/plain')],
        }
    }
    cherrypy.quickstart(InstaTM(), '/', conf)

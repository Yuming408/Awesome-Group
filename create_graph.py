__author__ = 'yumingfang'

import json
import networkx as nx

filename = "/Users/yumingfang/PycharmProjects/data_acqu/tweet_data/bb.data"

def parse_json_rest(filename):
    data = []

    with open(filename, 'rb') as f:
        for line in f:
            data.append(json.loads(line))
    return data


def create_graph(data):

    G = nx.DiGraph()

    #print len(data)
    #count = []

    for index in range(len(data)):


        # if data[index]['in_reply_to_user_id'] is not None:
        #     count.append(data[index]['in_reply_to_user_id'])
        mention = data[index]['entities']['user_mentions']
        #retweet = data[index]['retweeted']
        #fav = data[index]['favorited']
        if mention:
            print mention

        id = data[index]['user']['id']
        tz = data[index]['user']['time_zone']
        user_name = data[index]['user']['screen_name']
        reply_id = data[index]['in_reply_to_user_id']

        G.add_node(id)
        if tz is not None:
            G.node[id]['time_zone'] = tz
        if user_name is not None:
            G.node[id]['user_name'] = user_name


        if (reply_id is not None and reply_id != id):
            G.add_edge(reply_id, id)
            G.add_node(reply_id, user_name=data[index]['in_reply_to_screen_name'])

        if mention:
            u = mention[0]['id']
            if u != id:
                G.add_edge(u, id)
                G.add_node(u, user_name=mention[0]['screen_name'])


    return G




def main():
    data = parse_json_rest(filename)

    G = create_graph(data)

    nx.write_graphml(G, "test.graphml")


main()






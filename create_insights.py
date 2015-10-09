import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from collections import Counter
import random


def insight_1(filename):
    #filename = './result/distribution_by_tweets.csv'
    data1 = pd.read_csv(filename)
    candiate_name = data1.ix[:,0]
    data1 = data1.set_index(candiate_name)
    count = data1.ix[:,1]

    series1 = pd.Series(count,index=candiate_name)

    series1.plot(kind = 'bar', color = 'k')
    plt.show()
    #
    # data1.plot(kind = 'bar', color = 'k')
    # plt.xticks(range(len(candiate_name)), candiate_name)
    # plt.show()


    series1.plot(kind='pie', autopct='%.2f')
    plt.show()
def word_cloud(filename, name):
    tweets_text = pd.read_csv(filename).ix[:,0]
    name = name.rstrip().split(' ')
    words = ' '.join(tweets_text)
    words_filtered_list = [word for word in words.split()
                            if  word != name[0]
                                and word != name[1]
                                and  word != 'president'
                                and len(word) > 3
                            ]
    words_filtered = " ".join(words_filtered_list)
    top_words = Counter(words_filtered_list).most_common(10)
    print top_words
    wordcloud = WordCloud(
                      stopwords=STOPWORDS,
                      background_color='black',
                      max_words = 100,
                      width=1800,
                      height=1400
                     ).generate(words_filtered)

    plt.imshow(wordcloud.recolor(color_func=grey_color_func, random_state=3))
    #plt.imshow(wordcloud.recolor(random_state=3))

   # plt.imshow(wordcloud)
    plt.axis('off')
   # plt.savefig('./my_twitter_wordcloud_1.png', dpi=300)
    plt.show()

def grey_color_func(word, font_size, position, orientation, random_state=None, **kwargs):
    return "hsl(0, 0%%, %d%%)" % random.randint(60, 100)

def time_trend(filename):
    pd_candiate = pd.read_csv(filename)
    date = pd_candiate.ix[:,0]
    pd_candiate = pd_candiate.set_index(date)
  #  pd_candiate = pd.Series(index = date)
    print pd_candiate
    pd_candiate.plot()
    plt.xticks(rotation=90)
    plt.show()



def main():

    word_cloud('./result/text_trump.csv', 'donald trump')
    word_cloud('./result/text_hillary.csv', 'hillary clinton')
    #time_trend('./result/count_time_trend.csv')

if __name__ == '__main__':
    main()
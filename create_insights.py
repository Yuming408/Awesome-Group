import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():
    filename_1 = './result/distribution_by_tweets.csv'
    data1 = pd.read_csv(filename_1)
    candiate_name = data1.ix[:,0]
    data1 = data1.set_index(candiate_name)
    count = data1.ix[:,1]

    series1 = pd.Series(count,
                        index=candiate_name,
                       )

    print series1

    series1.plot(kind = 'bar', color = 'k')
    plt.show()
    #
    # data1.plot(kind = 'bar', color = 'k')
    # plt.xticks(range(len(candiate_name)), candiate_name)
    # plt.show()


    series1.plot(kind='pie', autopct='%.2f')
    plt.show()

    #
if __name__ == '__main__':
    main()
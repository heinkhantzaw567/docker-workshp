import  sys
import pandas as pd 





month = int(sys.argv[1])

df = pd.DataFrame({'day': [1, 2, 3], 'num_passengers': [10, 20, 30]})

print(df.head())


df.to_parquet(f'passengers_month_{month}.parquet')

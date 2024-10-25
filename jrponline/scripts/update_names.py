import pandas as pd

new_df = pd.read_csv('data/scraped/Final_Pricing_Model_Rounded_99.csv')
duplicate_mask = new_df.duplicated('DESCRIPTION', keep=False)
new_df['Modified Product Name'] = new_df['DESCRIPTION'].where(~duplicate_mask, new_df['DESCRIPTION'] + ' - ' + new_df['OEMSKU'])
new_df.to_csv('data/scraped/Updated_Final_Pricing_Model_Rounded_99.csv', index=False)
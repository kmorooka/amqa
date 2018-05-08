curl -o ./amqa-tmp.csv https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.csv
sed -e '1,5d' ./amqa-tmp.csv > ./amqa-tmp2.csv
sed -e 's/),/) /g' ./amqa-tmp2.csv > ./amqa-price.csv
rm -f ./amqa-tmp.csv ./amqa-tmp2.csv


#!/usr/bin/python 
# coding:utf-8
"""
File: amqa.py
Fuction: Execute SQL for Athena to summarize EC2 instance type & price.
Version: 1.1 (As of 2018.Apr)
"""
import sys
import time
import boto3
import subprocess
import csv
import pprint
# ----------------------------------------------------
# Please MODIFY before running the code !!
# Depends on the environment to use this tools.
# ----------------------------------------------------
S3BUCKET_NAME = 'amqa-sample'
REGION_NAME = 'ap-northeast-1'  # TOKYO-region
FN_RESULT = 'amqa-3year.csv'  # Result file name
S3WAIT_TIME = 5  # 5 sec wait for S3 sync. Too short to unfinish data.
# ----------------------------------------------------
# Change parms for each customer server csv file !!
# For Athena operations. Please MODIFY before running the code !!
# ----------------------------------------------------
DB_NAME = '"amqa-sample"."amqa_sample"'
CPU_CORE = 'vcpu'
MEM_SIZE = 'memory'
DISK_COL = 9  # Column position of disk size: Depends on customer csv file!! Start from Zero(0).
CPU_USAGE = 1.0  # Average 0-100% => 0.0-1.0

# ----------------------------------------------------
# Initial Setup - S3, Athena
# ----------------------------------------------------
athena = boto3.client('athena', region_name=REGION_NAME)
s3 = boto3.resource('s3')

# ----------------------------------------------------
# query_servers()
# Execute SQL for EC2 instance types.
# argument: (1) SQL cmd file to query Athena for EC2 sizing.
#       (2) Price dictionary pair with {ec2type : price}
# return: non
# output: 1 files. CSV server list files for each instance type.
# ----------------------------------------------------
def query_servers(fname, price_dict):
    
    ec2_finno = 0
    ec2_finprice = 0
    ebs_finno = 0
    ebs_finprice = 0
    
    #--- Create Result summary file & header line
    fd_result = open(FN_RESULT, 'w')
    fd_result.write('EC2-InstanceType,EC2-TotalInstance,EC2-UnitPrice(USD),EC2-TotalPrice(USD),EBS-TotalSize(G),EBS-TotalPrice(USD)\n')
    
    with open(fname, 'r') as fd: # Read SQL file & Exec query
        for line in fd.readlines():
            
            #------------------------------------------
            # Get SQL, query and store to S3.
            #------------------------------------------
            res = line.split(":")
            ec2_type = res[0]  # get ec2 instance type
            sql_cmd = res[1]   # get SQL statement
            sql_cmd = sql_cmd.replace("DB_NAME", DB_NAME)
            sql_cmd = sql_cmd.replace("CPU_CORE", CPU_CORE)
            sql_cmd = sql_cmd.replace("MEM_SIZE", MEM_SIZE)
            # print('amqa: sql_cmd = %s' % sql_cmd)  # debug
            result = athena.start_query_execution(
                QueryString = sql_cmd,
                ResultConfiguration = {
                    'OutputLocation': 's3://' + S3BUCKET_NAME})
                
            #------------------------------------------
            # Store to S3
            #------------------------------------------
            # print('amqa: Store S3')
            QueryExecutionId = result['QueryExecutionId']
            s3_key = QueryExecutionId + '.csv'
        
            #------------------------------------------
            # Wait writing sync with S3
            #------------------------------------------
            time.sleep(S3WAIT_TIME)
       
            #------------------------------------------
            # Download S3 result csv file to Local instance type file name csv. 
            #------------------------------------------
            local_fname = ec2_type + '.csv' 
            # print('amqa: s3_key = %s' % s3_key)
            # print('amqa: local_fname = %s' % local_fname)
            s3.Bucket(S3BUCKET_NAME).download_file(s3_key, local_fname)
            
            # Delete two temporary Athena result file.
            s3.Object(S3BUCKET_NAME, s3_key).delete()
            s3.Object(S3BUCKET_NAME, s3_key+'.metadata').delete()
            
            #------------------------------------------
            # EBS Total Disk Size. From downloaded result file.
            #------------------------------------------
            fd_ec2type = open(local_fname, 'r')
            #--- Get disk column
            size = 0
            sum_disk =  0
            disk_line = fd_ec2type.readline() # Skip file header.
            disk_line = fd_ec2type.readline()
            while disk_line:
                word = disk_line.split(",")
                col_disk = word[DISK_COL]
                col_disk = col_disk.replace("\"", "")
                # print(" col_disk = %s" % col_disk)
                if col_disk == "":
                    size = 0
                else:
                    size = int(col_disk)
                sum_disk += size
                disk_line = fd_ec2type.readline()
        
            #------------------------------------------
            # EC2 Total Price - 3yr,All upfront,Tokyo etc.
            #------------------------------------------
            # pprint.pprint(price_dict)
            ec2_price = price_dict.get(ec2_type)
            if ec2_price == None:
                ec2_price = 0
            #------------------------------------------
            # EC2 Total instance num.
            #------------------------------------------
            cmd = 'wc -l ' + local_fname
            rc = subprocess.getoutput(cmd)
            # print('rc = %s' % rc)
            line_count = rc.split(" ")
            ec2_num = CPU_USAGE * (int(line_count[0]) - 1)  # -1 for header count.
            #------------------------------------------
            # EBS Total disk price.
            #------------------------------------------
            ebs_price = get_gp2_price(sum_disk)
            #------------------------------------------
            # Create log line to write to result fil.
            #------------------------------------------
            ec2_totalprice = ec2_num * int(ec2_price)
            log_str = ec2_type + ',' + str(ec2_num) + ',' + str(ec2_price) + ',' \
            + str(ec2_totalprice) + ',' + str(sum_disk) + ',' + str(ebs_price) + '\n'
            print('amqa: Working {} ...'.format(ec2_type))
            fd_result.write(log_str)
            
            #------------------------------------------
            # Calc ALL instance No, Price.
            #------------------------------------------
            ec2_finno += ec2_num
            ec2_finprice += int(ec2_totalprice)
            ebs_finno += sum_disk
            ebs_finprice += ebs_price
            
            fd_ec2type.close()    
            
        # Write last line to Summary log.    
        log_str = 'TOTAL(EC2),' \
                  + str(ec2_finno) + ',' \
                  + str(0) + ',' \
                  + str(ec2_finprice) + ',' \
                  + str(ebs_finno) + ',' \
                  + str(ebs_finprice)
        fd_result.write(log_str)
        # print('debug: Exec TOTAL: {} '.format(log_str))
        
    fd_result.close()    
    
# ----------------------------------------------------
# create_pricing() 
# Create price dictionary using proice csv file.
# argument: price csv file name.
# return: dictionary {ec2 inst type : price }
# ----------------------------------------------------
def create_pricing(fn):
    
    PRICE = 'priceperunit'
    PRICE_POS = 9
    INSTANCETYPE = 'instance type'
    INSTANCETYPE_POS = 18
    TERMTYPE = 'Reserved'
    TERMTYPE_POS = 3
    LEASECONTRACTLENGTH = '3yr'
    LENGTH_POS =  11
    PURCHASEOPTION = 'All Upfront'
    PURCHASE_POS =  12
    LOCATION = 'Aisa Pacific (Tokyo)'
    LOCATION_POS = 16
    TENANCY = 'Shared'
    TENANCY_POS =  35
    OS = 'Linux'  # SUSE  RHEL  Linux  Windows
    OS_POS =  37
    OFFERINGCLASS = 'standard'
    OFFERINGCLASS_POS =  13
    UNIT = 'Quantity'
    UNIT_POS =  8
    price = 0 # Inisialize
    price_dict = {}
    
    with open(fn, mode='r') as fd:
        reader = csv.reader(fd)
        next(reader)  # Skip Header.
        for row in reader:
            if row[TERMTYPE_POS] == 'Reserved' \
            and row[LOCATION_POS] == 'Asia Pacific (Tokyo)' \
            and row[LENGTH_POS] == '3yr' \
            and row[PURCHASE_POS] == 'All Upfront' \
            and row[TENANCY_POS] == 'Shared' \
            and row[OFFERINGCLASS_POS] == 'standard' \
            and row[UNIT_POS] == 'Quantity' \
            and row[OS_POS] == 'Linux':
                price = row[PRICE_POS]
                ec2type = row[INSTANCETYPE_POS]
                price_dict[ec2type] = price
    # pprint.pprint(price_dict)
    return(price_dict)

# ----------------------------------------------------
# get_gp2_price() 
# Calculate total gp2 price. Tokyo $0.12/G/1month.
# argument: total disk size(G)
# return: total GP2 EBS prcie(USD)
# ----------------------------------------------------
def get_gp2_price(disk_size):
    
    unit_price = 0.12  # EBS-GP2 $0.12/month/Gbytes
    gp2_price = unit_price * disk_size * 36  # 36 months, 3yr
    return(gp2_price)

# ----------------------------------------------------
# main() 
# ----------------------------------------------------
def main():
    
    argv = sys.argv
    argc = len(argv)
    if argc != 3:
        print('amqa: ERROR: Usage $./amqa.py <sql cmd file> <price list file> (Exp) cargo.py amqa-sql-ec2.txt price.csv')
        sys.exit('amqa: ERROR.')
    print("amqa: Aws Migration Quick Assessment Sizing Tool - STARTING...\n")
    price_dict = create_pricing(argv[2])
    query_servers(argv[1], price_dict)
    print("amqa: Check the file = {}\n".format(FN_RESULT))
    sys.exit('amqa: END Successfully.')

# ----------------------------------------------------
if __name__ == '__main__':
    main()
    
# ----------------------------------------------------
# End of File
# ----------------------------------------------------

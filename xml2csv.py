'''colocar xml2csv.py e o documento AMBIDATA_500043_20180329_015953.xml  no mesmo directorio
   Executar no terminal  $ python xml2csv.py > result.csv 
'''
import sys
import xml.etree.ElementTree as ET
from optparse import OptionParser


def main():
    parser = OptionParser(
        usage="usage: %prog [options] filename", version="%prog 1.0")
    parser.add_option("-i", "--input",
                      action="store_true",
                      dest="xml_flag",
                      default=False,
                      help="input file")
    parser.add_option("-o", "--output",
                      action="store",  # optional because action defaults to "store"
                      dest="output_filename",
                      default="output.csv",
                      help="Send the output to specific file",)
    (options, args) = parser.parse_args()
    # print(options)
    # print(args[0])

    if len(args) != 1:
        parser.error("wrong number of arguments")

    inputFile = args[0]
    outputFile = options.output_filename
    #print("input ", inputFile)
    #print("output", outputFile)

    transform(inputFile, outputFile)


def transform(inputFile, outputFile):
    #inputFile = 'resident_data.xml'
    try:
        tree = ET.parse(inputFile)

        root = tree.getroot()

        TXN_CODE = TXN_LINE_CODE = LOCATION_CODE = EMPLOYEE_CODE = CUSTOMER_CODE = TXN_DATE = PRODUCT_CODE = EVENT_CODE = TICKET_TYPE_CD = SLS_QTY = NET_SLS_VALUE = GROSS_SLS_VALUE = VAT = CURRENCY_CODE = PAYMENT_METHOD = TXN_LINE_DATE = ""
        header = "TXN_CODE;TXN_LINE_CODE;LOCATION_CODE;EMPLOYEE_CODE;CUSTOMER_CODE;TXN_DATE;PRODUCT_CODE;EVENT_CODE;TICKET_TYPE_CD;SLS_QTY;NET_SLS_VALUE;GROSS_SLS_VALUE;VAT;CURRENCY_CODE;PAYMENT_METHOD;TXN_LINE_DATE"

        # save output
        oldstdout = sys.stdout
        # Redirect output to file
        sys.stdout = open(outputFile, 'w')
        print(header)										# with header

        for store in root:
            LOCATION_CODE = store.get('id')  				# External location code
            CURRENCY_CODE = store.get('currency')
            for date in store.findall('date'):
                day = date.get('day')  						# gets the day
                for sales in date:
                    for sale in sales.findall('sale'):
                        time = sale.get('time')  			# gets the time

                        # 2017-02-27T00:00.00Z   Date when ticket/transaction took place
                        TXN_DATE = day+"T"+time+"Z"
                        TXN_LINE_DATE = TXN_DATE

                        # External ticket/transaction code
                        TXN_CODE = sale.find('id').text

                        for ticketRow in sale.findall('ticketRow'):

                            # External ticket/transaction line code
                            TXN_LINE_CODE = ticketRow.find('rowId').text

                            # Number of product units
                            SLS_QTY = ticketRow.find('soldQty').text
                            VAT = ticketRow.find(
                                'taxPercentage').text  		# VAT %
                            GROSS_SLS_VALUE = ticketRow.find(
                                'itemAmount').text 	 							# Sales with VAT
                            NET_SLS_VALUE = (float(ticketRow.find(
                                'itemAmount').text) - float(ticketRow.find('taxAmount').text))  # Sales without VAT

                            # TICKET_TYPE_CD    (S)ale or (R)efund or (A)dvance
                            if(int(ticketRow.find('soldQty').text) < 0):
                                TICKET_TYPE_CD = "R"
                            else:
                                TICKET_TYPE_CD = "S"

                            for sku in ticketRow.findall('SKU'):
                                # External product identifier.
                                PRODUCT_CODE = sku.find('collection').text + sku.find(
                                    'style').text + sku.find('color').text + sku.find('size').text

                            # print to outputfile
                            print(TXN_CODE+";"+TXN_LINE_CODE+";"+LOCATION_CODE+";"+EMPLOYEE_CODE+";"
                                  + CUSTOMER_CODE+";"+TXN_DATE+";"+PRODUCT_CODE+";"+EVENT_CODE+";"
                                  + TICKET_TYPE_CD+";"+SLS_QTY+";"+"%.2f" % NET_SLS_VALUE+";"+GROSS_SLS_VALUE+";"
                                  + VAT+";"+CURRENCY_CODE+";"+PAYMENT_METHOD+";"+TXN_LINE_DATE)

        # redirect output back to terminal
        #sys.stdout = sys.__stdout__
        # print("DONE")

    except FileNotFoundError:
        print("File name does not exist; please try again")
    except IOError:
        print('Invalid File')


if __name__ == '__main__':
    main()

import mrjob
from mrjob.job import MRJob
from mrjob.step import MRStep
#对原始数据预处理
def formatd(_row):
  return _row['Date received']+'|'+_row['Product'].lower()+'|'+_row['Company']+'\n'
import csv

with open('complaints.csv','r', encoding='UTF-8') as fi, open('output.csv', 'w',encoding='UTF-8') as fo:
  reader = csv.DictReader(fi)
  for row in reader:
    fo.write(formatd(row))

class complaintsCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_csvrow,
                   # combiner=self.combiner_count_complaints,
                   reducer=self.reducer_count_complaints),
            MRStep(reducer=self.reducer_result_complaints)
        ]
    def mapper_get_csvrow(self, _, line):
        rows = line.split("|")
        tempyear = rows[0].split("-")[0]
        tempkey = rows[1].lower() + "&" + tempyear
        yield tempkey,(rows[1].lower(), tempyear, 1, rows[2])
    def reducer_count_complaints(self, tempkey,comps):
        cout_num=0
        comp_company=[]
        comp_pro=""
        comp_year=""
        for product,year,counts,company in comps:
            cout_num=cout_num+counts
            comp_company.append(company)
            comp_pro=product
            comp_year=year
        yield tempkey,(comp_pro, comp_year, cout_num, comp_company)

    def reducer_result_complaints(self, tempkey,comps):
        retcomps=()
        for product, year, counts, company in comps:
            company_num= len(list(set(company)))
            maxcom = max(set(company), key=company.count)
            tempcou = round((str(company).count(maxcom) / len(company)) * 100)
            retcomps=(product,year,counts,company_num,tempcou)
        yield tempkey, retcomps
if __name__ == '__main__':
    complaintsCount.run()
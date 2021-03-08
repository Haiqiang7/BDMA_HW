from mrjob.job import MRJob
from mrjob.step import MRStep


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
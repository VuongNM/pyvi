import codecs
import multiprocessing as mp
from pyvi.pyvi import ViTokenizer
import datetime 
import sys 


output_file = None 

def process_chunk(chunk):
    print "Process chunk of size "+ str(len(chunk))
    ret = []
    for line in chunk:
        line = line.strip()
        if len(line) > 0:
            out = ViTokenizer.tokenize( line )
            ret.append(out)
    return ret

def write_file_callback(ret):
    for i in ret:
        output_file.write(i + "\n")



def main(path,output_path=None, num_worker=1,encoding='utf-8'):

    counter = 0 
    batch = []
    batch_size = 10000
    pool = mp.Pool(num_worker)

    f = codecs.open(path,encoding=encoding,mode="r")

    starttime  = datetime.datetime.now()
    for line in f :
        batch.append(line)
        if len(batch) == batch_size :
            pool.apply_async(process_chunk,args=(batch,), callback = write_file_callback)
            batch = []

    pool.apply_async(process_chunk,args=(batch,), callback = write_file_callback)

    pool.close()
    pool.join()

    endtime  = datetime.datetime.now()
    print "TIME CONSUMED " + str(endtime - starttime) #5 sec/1k record



if __name__ =="__main__":
    if len(sys.argv) == 4:
        input_path = sys.argv[1]
        ouput_path = sys.argv[2]
        num_worker = int (sys.argv[3])
        print input_path
        print output_path
        print num_worker
        output_file = codecs.open(output_path,mode="w",encoding="utf-8")

        main(path=input_path,output_path=output_path,num_worker=num_worker)
    else:
        print """
            python pyvi_largefile.py <input> <output> <num_worker>
            numworker : number of process to deal with the file
        """

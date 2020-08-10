//readme
//This is an example use case only

//cd to directory where files are located

//start slave processes
q .mi.worker.q -p 5100
q .mi.worker.q -p 5101
q .mi.worker.q -p 5102

//start master process
q .mi.runOrchestrator.q -p 5000

//load hdb
cd hdb
q .

//query to confirm data looks good
select count i by date from marketTrades

from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

#TSocket.TServerSocket(host='master', port=9090)
socket = TSocket.TSocket('master',9090)
socket.setTimeout(5000)

transport = TTransport.TBufferedTransport(socket)
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Hbase.Client(protocol)
socket.open()

tables = client.getTableNames()
for item in tables:
	print str(item)
#print client.get('test','row2','cf:a')

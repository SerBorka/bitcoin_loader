from bitcoincli import Bitcoin

host = "nd-312-586-324.p2pify.com"
port = "80"
username = "vigilant-keller"
password = "anyone-uproar-tabby-skier-parted-decade"

bitcoin = Bitcoin(username, password, host, port)
info = bitcoin.getblockchaininfo()
print(info)
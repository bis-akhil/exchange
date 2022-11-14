class LOB:
    order_id = 1
    def __init__(self, base_value):
        self.spread = 0
        self.buy_order = 0#core_var["buy_order"]
        self.sell_order = 0#core_var["sell_order"]
        self.buy_book = []
        self.sell_book = []
        self.price_hist = [base_value]
        self.mid_price_hist = [base_value]
        
        
    def limit_order(self,order): 
        if(order['buy']):
            message = self.buy_limit_order(order)
            return message
        else:
            message = self.sell_limit_order(order)
            return message
        
        
    def buy_limit_order(self,order):
        message = ''
        while True:
            if(self.sell_book):
                if(self.sell_book[0][1]<=order['price']):
                    self.price_hist.append(self.sell_book[0][1])
                    if self.sell_book[0][2]>order['vol']:
                        self.sell_book[0][2]-=order['vol']
                        order['fulfilled'] = True
                        return "order completed"
                    elif self.sell_book[0][2]==order['vol']:
                        self.sell_book.pop(0)
                        self.sell_order-=1
                        order['fulfilled'] = True
                        self.update_spread()
                        self.update_mid_price()
                        return "order completed"
                    else:
                        order['vol']=order['vol']-self.sell_book[0][2]
                        self.sell_book.pop(0)
                        self.sell_order-=1
                        message += "order partially completed "
                else:
                    break
            else:
                break
        if not order['fulfilled']:
            for i in range(self.buy_order):
                if(self.buy_book[i][1]<order['price']):
                    self.buy_book.insert(i,[self.order_id,order['price'],order['vol']])                     
                    break
            else:
                self.buy_book.append([self.order_id,order['price'],order['vol']])
            self.update_spread()
            self.update_mid_price()
            self.buy_order+=1
            self.order_id+=1
            message+="order added to lob"
            return (message,"b"+str(self.order_id-1))
        
    def sell_limit_order(self,order):
        message = ''
        while True:
            if(self.buy_book):
                if(self.buy_book[0][1]>=order['price']):
                    self.price_hist.append(self.buy_book[0][1])
                    if self.buy_book[0][2]>order['vol']:
                        self.buy_book[0][2]-=order['vol']
                        order['fulfilled'] = True
                        return "order completed"
                    elif self.buy_book[0][2]==order['vol']:
                        self.buy_book.pop(0)
                        self.buy_order-=1
                        order['fulfilled'] = True
                        self.update_spread()
                        self.update_mid_price()
                        return "order completed"
                    else:
                        order['vol']=order['vol']-self.buy_book[0][2]
                        self.buy_book.pop(0)
                        self.buy_order-=1
                        message += "order partially completed "
                else:
                    break
            else:
                break

        if not order['fulfilled']:
            for i in range(self.sell_order):
                if(self.sell_book[i][1]>order['price']):
                    self.sell_book.insert(i,[self.order_id,order['price'],order['vol']])
                    break
            else:
                self.sell_book.append([self.order_id,order['price'],order['vol']])
            self.update_spread()
            self.update_mid_price()
            self.sell_order+=1
            self.order_id+=1
            message+="order added to lob"
            return (message,"s"+str(self.order_id-1))
    

    
    def update_spread(self):
        if self.sell_book and self.buy_book:
            self.spread = self.sell_book[0][1] - self.buy_book[0][1]
            
    def clear_lob(self):
        # self.spread = 0
        self.buy_order = 0
        self.sell_order = 0
        self.buy_book = []
        self.sell_book = []
        self.order_id = 1
        
    def update_mid_price(self):
        if self.sell_book and self.buy_book:
            new_mid_price = (self.sell_book[0][1] + self.buy_book[0][1])/2
            self.mid_price_hist.append(new_mid_price)

        
    def __str__(self):
        return f'''
        Number of buy orders, {self.buy_order} \n
        Number of sell order, {self.sell_order} \n
        Bid-Ask spread, %.2f \n
        Mid price, %.2f \n
        Last price, %.2f \n     ''' %(self.spread,self.mid_price[-1],self.price_hist[-1])
    
    
    # def send_data(self):
    #     self.kafka_bus.receive_data_lob(self.mid_price,self.price_hist[-1])
        
        
        
    def __del__(self):
        del self.spread
        # del self.isempty
        del self.buy_order
        del self.sell_order
        del self.buy_book
        del self.sell_book
        # print("deleted")
        
        
        
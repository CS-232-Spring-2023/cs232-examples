from abc import ABC
from abc import abstractmethod

class TradingBot(ABC):
    
    def connect(self):
        print(f"Connecting to Crypto exchange...")

    def get_market_data(self):
        return [10, 12, 18, 14]

    def check_prices(self, coin):
        self.connect()
        prices = self.get_market_data()
        should_buy = self.should_buy(prices)
        should_sell = self.should_sell(prices)
        if should_buy:
            print(f"You should buy {coin}!")
        elif should_sell:
            print(f"You should sell {coin}!")
        else:
            print(f"No action needed for {coin}.")

    @abstractmethod
    def should_buy(self, prices) -> bool:
        pass

    @abstractmethod
    def should_sell(self, prices) -> bool:
        pass


class AverageTrader(TradingBot):

    def list_average(self, price_list):
        return sum(price_list) / len(price_list)

    def should_buy(self, prices):
        return prices[-1] < self.list_average(prices)

    def should_sell(self, prices):
        return prices[-1] > self.list_average(prices)


class MinMaxTrader(TradingBot):

    def should_buy(self, prices):
        return prices[-1] == min(prices)

    def should_sell(self, prices):
        return prices[-1] == max(prices)


def main():
    application = AverageTrader()
    application.check_prices("BTC/USD")


if __name__ == "__main__":
    main()

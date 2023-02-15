from abc import ABC
from abc import abstractmethod
import os


class Subject(ABC):

    @abstractmethod
    def attach(self, observer) -> None:
        pass

    @abstractmethod
    def detach(self, observer) -> None:
        pass

    @abstractmethod
    def notify(self) -> None:
        pass

class LogData(Subject):
    def __init__(self, log_filename):
        self.message = None
        self._observers = []
        self._log_filename = self._get_absolute_file_path(log_filename)

    def attach(self, observer) -> None:
        print(f"Attach {type(observer).__name__} observer")
        self._observers.append(observer)

    def detach(self, observer) -> None:
        print(f"Remove {type(observer).__name__} observer")
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.update(self)

    def read_log_file(self) -> None:
        with open(self._log_filename, 'r') as input_file:
            for line in input_file:
                self.message = line.strip()
                self.notify()
    
    def _get_absolute_file_path(self, filename):
        absolute_path_to_script = os.path.realpath(__file__)
        absolute_path_to_directory = os.path.dirname(absolute_path_to_script)
        return os.path.join(absolute_path_to_directory, filename)


class Observer(ABC):
    @abstractmethod
    def update(self, subject) -> None:
        pass


class ErrorListener(Observer):
    def update(self, subject) -> None:
        if "ERROR" in subject.message:
            print(f"{self.__class__.__name__} received: \"{subject.message}\"")


class LogListener(Observer):
    def update(self, subject) -> None:
        print(f"{self.__class__.__name__} received: \"{subject.message}\"")


def main():
    subject = LogData("log.txt")
    error_observer = ErrorListener()
    subject.attach(error_observer)
    log_data_observer = LogListener()
    subject.attach(log_data_observer)
    subject.read_log_file()

    # Only Error Listener
    subject.detach(log_data_observer)
    subject.read_log_file()


if __name__ == "__main__":
    main()

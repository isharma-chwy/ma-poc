from abc import ABC, abstractmethod


class AbstractBIGetter(ABC):
    """
    Abstract base class defining the interface for a BI getter.
    """

    @abstractmethod
    def connect(self):
        """
        Establish a connection to the BI system.
        """
        pass

    @abstractmethod
    def execute_query(self, query, params, debug):
        """
        Execute a query against the BI system.
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Disconnect from the BI system.
        """
        pass

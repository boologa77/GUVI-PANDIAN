


# abstract class are imported through abc which are abstract base classes 

# Each abstract class will have one abstract method which is also again imported through abstract method

# what is abstract class and abstract method each class will have one abstract method and that method will have declaration but not the definition


from abc import ABC,abstractmethod


class computer(ABC):
    
    @abstractmethod
    def process(self):
        pass
    

class student(computer):
    def process(self):
        print('running')
    
    
    
# a=student()
# a.process()

# b=computer()




from abc import ABC, abstractmethod  # Import ABC and abstractmethod from the abc module

# Define an abstract class using ABC (Abstract Base Class)
class Animal(ABC):
    # Define an abstract method using the @abstractmethod decorator
    @abstractmethod
    def sound(self):
        pass  # Abstract methods don't have an implementation

# Define a subclass of Animal
class Dog(Animal):
    # Implement the abstract method in the subclass
    def sound(self):
        return "Bark"

# Define another subclass of Animal
class Cat(Animal):
    # Implement the abstract method in the subclass
    def sound(self):
        return "Meow"

# Create instances of the subclasses
dog = Dog()  # Instantiate the Dog class
cat = Cat()  # Instantiate the Cat class

# Call the sound method on the instances
print(dog.sound())  # Output: Bark
print(cat.sound())  # Output: Meow

# Summary of Comments:
# - from abc import ABC, abstractmethod: Imports necessary tools for creating abstract classes.
# - class Animal(ABC): Defines an abstract base class (ABC) named Animal.
# - @abstractmethod: Marks a method as abstract, meaning it must be implemented by any subclass.
# - pass: Abstract methods don't have a body; they are defined in the subclasses.
# - class Dog(Animal): A subclass of Animal that implements the abstract method.
# - class Cat(Animal): Another subclass of Animal that implements the abstract method.
# - dog = Dog() and cat = Cat(): Creates instances of Dog and Cat.
# - dog.sound() and cat.sound(): Calls the sound method, which is defined in the subclasses.

# Abstract classes cannot be instantiated directly and are used to define common interfaces for subclasses. Subclasses must provide implementations for all abstract methods.

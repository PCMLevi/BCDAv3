from prefect import flow, task

@task
def greet(name: str) -> str:
    return f"Hello, {name}!"

@flow
def hello_flow(name: str = "world"):
    greeting = greet(name)
    print(greeting)
    return greeting

if __name__ == "__main__":
    hello_flow("Alice")
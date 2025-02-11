This is a small implementation of an [Interval](https://interval.com/) like system in Python. You write workflows (called as "actions") as async routines which suspend every time they need a human input. Each execution of an action is called a "transaction". The framework takes care of rendering the UI, collecting the input, and resuming the workflow. 

The Interval github org also has a [mini version](https://github.com/interval/mini/) of it, where the "actions" code the one developers are supposed to write, and the Interval "server", which is connected to by the end users are run in the same server. This makes things easier to run, if less scalable. 

TODO: Acquire a lock on a transaction, so you only have one instance of it running at a time.

## How to run

```bash
uv sync
uv run sdk.py # implements some toy actions (workflows)
```

Access the UI in the web browser at `http://localhost:8000`.

## Context for LLM

<details>
<summary>ChatGPT Generated description of Interval</summary>

Interval is a framework for building internal tools and operational workflows that allows developers to create interactive, multi-step processes using code rather than drag-and-drop interfaces. The key aspects of Interval's approach are:

The framework provides a set of pre-built UI components and interactive elements that can be composed programmatically. These include:
- Form inputs (text fields, selects, file uploads, etc.)
- Data display components (tables, charts, JSON viewers)
- Interactive elements (confirmation dialogs, action buttons)
- Layout components for organizing information

Rather than building static pages, developers create "actions" - interactive procedures that can request input from users, display information, and execute backend logic in a defined sequence. The framework handles the state management and UI rendering automatically.

A typical Interval action might:
1. Display a form to collect initial parameters
2. Make API calls or database queries based on that input
3. Show the results in a table
4. Allow selecting rows for further processing
5. Confirm dangerous operations
6. Execute final changes and show success/failure states

The framework manages the execution flow, allowing actions to pause and wait for user input before continuing. This creates an interactive, wizard-like experience where complex operations can be broken down into clear steps.

Interval also provides:
- Authentication and user management
- Logging of all actions and their outcomes
- The ability to schedule actions to run on a recurring basis
- A dashboard to browse and execute available actions
- Role-based access control to restrict who can run specific actions

The core philosophy is that internal tools should be built with the same engineering practices as production code - version control, testing, code review, etc. - while still providing a polished user experience. This contrasts with no-code tools that prioritize rapid development but can be limiting and hard to maintain as requirements grow more complex.

By using code, developers can:
- Implement complex business logic
- Interface directly with existing services and databases
- Handle edge cases and errors gracefully
- Maintain consistency with other backend systems
- Leverage existing development workflows
- Create reusable components and utilities

This design makes Interval particularly well-suited for operations that:
- Require significant business logic or data processing
- Need to integrate with multiple systems
- Have complex validation or authorization requirements
- Would benefit from being broken into discrete steps
- Need to be maintainable by engineers long-term

The framework essentially provides the infrastructure layer (state management, UI rendering, authentication, etc.) while letting developers focus on implementing the actual business logic in a natural, programmatic way.
</details>